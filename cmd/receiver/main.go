// Receiver 入口：III 区接收帧 → 校验/去重 → 原样 import 写目标 VictoriaMetrics → ACK。
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
	"vm-sync/internal/receiver"
	"vm-sync/internal/transport"
	"vm-sync/internal/vm"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "receiver: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/receiver.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.LoadReceiver(cfgPath)
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Log)
	if err != nil {
		return err
	}
	defer log.Sync()
	log.Info("receiver starting",
		zap.String("target", cfg.Target.URL),
		zap.String("listen", cfg.TCP.Listen),
	)

	// 目标 VictoriaMetrics
	vmClient, err := vm.NewClient(cfg.Target)
	if err != nil {
		return err
	}

	// 监控指标
	metrics := monitor.New()

	// 帧处理器（校验/去重/写库/ACK）；A5：落库点时间戳 → e2e 延迟指标
	rcfg := cfg.ReceiverConfig()
	rcfg.LastWriteTs = metrics.SetLastWriteTs
	handler, err := receiver.New(vmClient, metrics, log, rcfg)
	if err != nil {
		return err
	}

	// TCP 服务器（frameIdx：连接内数据帧到达序号，供 receiver 缺口闭合判定首帧）
	srv := transport.NewServer(cfg.ServerConfig(), func(connID uint64, frameIdx uint64, frameBytes []byte) byte {
		return handler.HandleFrame(connID, frameIdx, frameBytes)
	})
	if err := srv.Listen(); err != nil {
		return fmt.Errorf("listen %s: %w", cfg.TCP.Listen, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Serve(ctx)

	// 指标 HTTP 服务
	metricsSrv := metrics.NewHTTPServer(cfg.Monitor.Addr, cfg.Monitor.Auth())
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil {
			log.Warn("metrics server stopped", zap.Error(err))
		}
	}()
	log.Info("receiver started", zap.String("listen", cfg.TCP.Listen), zap.String("metrics", cfg.Monitor.Addr))

	// 优雅退出
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Info("shutting down", zap.String("signal", s.String()))
	cancel()
	srv.Close()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	metricsSrv.Shutdown(shutCtx)
	log.Info("receiver stopped")
	return nil
}
