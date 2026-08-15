// Sender 入口：I 区数据轮询（export）→ WAL → 隔离发送。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
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

	// 回填策略（V1.7 同款语义）：all=全量（默认）/ 0=仅实时 / Nd=有界（d 单位）。
	// 数据起点探测：定位库内最早数据，避免"回拨边界早于实际数据"时空爬。
	spec := cfg.BackfillSpec()
	now := time.Now().UnixMilli()
	var oldest int64
	if spec.Mode != config.BackfillNone {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
		if o, err := vmClient.ProbeOldestData(probeCtx); err == nil {
			oldest = o
		} else {
			log.Warn("probe oldest data failed, use configured boundary only", zap.Error(err))
		}
		probeCancel()
	}
	policyNs, boundaryNs := int64(0), int64(0)
	switch spec.Mode {
	case config.BackfillAll:
		policyNs, boundaryNs = wal.BackfillAllNs, oldest
	case config.BackfillDuration:
		policyNs = spec.Dur.Milliseconds()
		boundaryNs = now - policyNs
		if oldest > 0 && oldest > boundaryNs {
			boundaryNs = oldest // 真空区直接越过
		}
	}
	rewound, err := walInst.ApplyBackfillPolicy(policyNs, boundaryNs)
	if err != nil {
		return fmt.Errorf("apply backfill policy: %w", err)
	}
	if rewound {
		log.Info("backfill policy changed: cursor rewound",
			zap.Int64("cursor", walInst.Cursor()), zap.Int64("boundary", boundaryNs))
	}

	// 首次启动（无 checkpoint）：游标 = 回填边界 / now-watermark
	if walInst.Cursor() == 0 {
		init := now - cfg.WatermarkDuration().Milliseconds()
		switch spec.Mode {
		case config.BackfillAll:
			if oldest > 0 {
				init = oldest
			}
		case config.BackfillDuration:
			if boundaryNs > 0 {
				init = boundaryNs
			}
		}
		if err := walInst.SetCursor(init); err != nil {
			return fmt.Errorf("init cursor: %w", err)
		}
		log.Info("first start: cursor initialized",
			zap.Int64("cursor", init), zap.String("backfill_mode", fmt.Sprint(spec.Mode)))
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

	go poller.Run(ctx)
	go senderLoop.Run(ctx)

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
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	metricsSrv.Shutdown(shutCtx)
	client.Close()
	log.Info("sender stopped")
	return nil
}
