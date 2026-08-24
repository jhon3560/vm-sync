// Sender 入口：I 区数据轮询（export）→ WAL → 隔离发送。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"vm-sync/internal/config"
	"vm-sync/internal/logger"
	"vm-sync/internal/monitor"
	"vm-sync/internal/sender"
	"vm-sync/internal/transport"
	"vm-sync/internal/vm"
	"vm-sync/internal/wal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "sender: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/sender.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.LoadSender(cfgPath)
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Log)
	if err != nil {
		return err
	}
	defer log.Sync()
	log.Info("sender starting",
		zap.String("source", cfg.Source.URL),
		zap.String("tcp", cfg.TCP.Addr),
		zap.String("wal", cfg.WAL.Path),
	)

	// VictoriaMetrics 源
	vmClient, err := vm.NewClient(cfg.Source)
	if err != nil {
		return err
	}

	// WAL
	walInst, err := wal.Open(cfg.WAL.Path, cfg.SegmentSize())
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	defer walInst.Close()

	// 回填策略与初始游标（R19/R23/R24 语义，见 backfillInit）
	spec := cfg.BackfillSpec()
	now := time.Now().UnixMilli()
	if err := backfillInit(log, vmClient, walInst, spec,
		cfg.EffectiveExportLag().Milliseconds(), now); err != nil {
		return err
	}
	log.Info("wal restored",
		zap.Int64("cursor", walInst.Cursor()),
		zap.Int("pending", walInst.PendingCount()),
		zap.Uint64("next_seq", walInst.NextSeq()),
	)

	// 监控指标
	metrics := monitor.New()
	metrics.SetCursor(walInst.Cursor())
	metrics.SetWALPending(int64(walInst.PendingCount()))
	metrics.SetWALBytes(walInst.DiskUsage())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 组件
	poller := sender.NewPoller(vmClient, walInst, metrics, log, cfg.PollerConfig())
	client := transport.NewClient(cfg.ClientConfig())
	senderLoop := sender.NewSender(walInst, client, metrics, log, cfg.SenderLoopConfig())

	// R18：poller/sender goroutine 生命周期追踪——优雅退出必须先等它们完全
	// 退出（cancel 后可被阻塞在 WaitAck 最长一个 TCP 超时）再关连接与 WAL：
	// 否则仍在运行的 goroutine 会使用已关闭的 WAL（错误风暴/半截持久化/
	// 目录锁已释放下的并发写风险）。
	var runWG sync.WaitGroup
	runWG.Add(2)
	go func() {
		defer runWG.Done()
		poller.Run(ctx)
	}()
	go func() {
		defer runWG.Done()
		senderLoop.Run(ctx)
	}()

	// 指标 HTTP 服务
	metricsSrv := metrics.NewHTTPServer(cfg.Monitor.Addr, cfg.Monitor.Auth())
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil {
			log.Warn("metrics server stopped", zap.Error(err))
		}
	}()
	log.Info("sender started", zap.String("metrics", cfg.Monitor.Addr))

	// 优雅退出
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Info("shutting down", zap.String("signal", s.String()))
	cancel()
	runWG.Wait() // R18：poller/sender 完全退出后才可关连接/WAL
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	metricsSrv.Shutdown(shutCtx)
	client.Close()
	log.Info("sender stopped")
	return nil
}

// backfillInit 应用回填策略与初始游标。
//
// R19：backfill=all 且最老数据探测失败时，本次启动**不记录/不应用**策略，
// 只从水位起同步实时数据（策略留待下次重启重试——游标回拨幂等安全）。
// 修复前探测失败仍会把 policy=-1/boundary=0 落盘：策略从此不再变化，
// backfill=all 静默退化为永久实时模式，历史数据永久漏发。
// 有界回填（Nd）不依赖探测（边界=now-Nd 必然有效），探测仅用于把边界
// 前伸到库内最早数据。
//
// R23：探测成功但库为空（oldest==0）同样**不记录**策略——否则 policy 与
// boundary=0 落盘后游标被永久锁在"启动即实时"，后续导入的历史数据在
// 重启后也不会回拨（e2e 实测空库首启 + 导入历史数据 + 重启 → 目标端 0 点）。
//
// R24：初始游标用有效导出余量（max(watermark, export_lag, 15s)）而非裸
// watermark——与 poller 窗口尾端钳位一致，避免前几轮 end<=cursor 空转。
func backfillInit(log *zap.Logger, vc *vm.Client, w *wal.WAL, spec config.BackfillSpec, initLagMs, now int64) error {
	var oldest int64
	probeOK := true
	if spec.Mode != config.BackfillNone {
		// 启动时序兜底：VM 自身 HTTP 可能仍慢一拍，探测带重试
		probeOK = false
		for attempt := 1; attempt <= 5; attempt++ {
			probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
			o, err := vc.ProbeOldestData(probeCtx)
			probeCancel()
			if err == nil {
				oldest = o
				probeOK = true
				break
			}
			log.Warn("probe oldest data failed, retrying", zap.Int("attempt", attempt), zap.Error(err))
			time.Sleep(time.Second)
		}
	}
	policyNs, boundaryNs := int64(0), int64(0)
	applyPolicy := true
	switch spec.Mode {
	case config.BackfillAll:
		if !probeOK {
			log.Error("backfill=all but oldest-data probe failed after retries; " +
				"policy NOT recorded this boot (retried on next restart), starting from watermark")
			applyPolicy = false
			break
		}
		if oldest == 0 {
			log.Info("backfill=all but database is empty; " +
				"policy NOT recorded this boot (re-applied on next restart once data exists), starting from watermark")
			applyPolicy = false
			break
		}
		policyNs, boundaryNs = wal.BackfillAllNs, oldest
	case config.BackfillDuration:
		if probeOK && oldest == 0 {
			log.Info("backfill duration configured but database is empty; " +
				"policy NOT recorded this boot (re-applied on next restart once data exists)")
			applyPolicy = false
			break
		}
		policyNs = spec.Dur.Milliseconds()
		boundaryNs = now - policyNs
		if probeOK && oldest > 0 && oldest > boundaryNs {
			boundaryNs = oldest // 真空区直接越过
		}
	}
	if applyPolicy {
		rewound, err := w.ApplyBackfillPolicy(policyNs, boundaryNs)
		if err != nil {
			return fmt.Errorf("apply backfill policy: %w", err)
		}
		if rewound {
			log.Info("backfill policy changed: cursor rewound",
				zap.Int64("cursor", w.Cursor()), zap.Int64("boundary", boundaryNs))
		}
	}
	if w.Cursor() == 0 {
		init := now - initLagMs
		switch {
		case spec.Mode == config.BackfillAll && probeOK && oldest > 0:
			init = oldest
		case spec.Mode == config.BackfillDuration && boundaryNs > 0:
			init = boundaryNs
		}
		if err := w.SetCursor(init); err != nil {
			return fmt.Errorf("init cursor: %w", err)
		}
		log.Info("first start: cursor initialized",
			zap.Int64("cursor", init), zap.String("backfill_mode", fmt.Sprint(spec.Mode)))
	}
	return nil
}
