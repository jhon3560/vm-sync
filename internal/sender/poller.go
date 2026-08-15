// Package sender 实现 I 区同步发送端：export 窗口轮询 → 分块 → WAL → 停等发送。
//
// 数据面零转换：VictoriaMetrics 的 /api/v1/export 输出与 /api/v1/import 输入是
// 完全对称的 JSON lines 格式——poller 只按"行数/字节"把导出结果重新分块进帧，
// 不解析、不重建任何样本数据；receiver 原样 POST /api/v1/import。
package sender

import (
	"bytes"
	"context"
	"syscall"
	"time"

	"go.uber.org/zap"

	"vm-sync/internal/monitor"
	"vm-sync/internal/protocol"
	"vm-sync/internal/vm"
	"vm-sync/internal/wal"
)

// PollerConfig 轮询配置。
type PollerConfig struct {
	Interval    time.Duration // 轮询周期，默认 500ms
	Window      time.Duration // 查询窗口，默认 5s
	Watermark   time.Duration // 水位延迟，默认 1s（VM 写入即查询可见，仅防时钟抖动）
	MaxWindow   time.Duration // 单次查询窗口上限（防时间跳变），默认 30s
	FrameLines  int           // 每帧最多 export 行数（每行可含多样本），默认 5000
	FrameBytes  int           // 每帧压缩前字节上限，默认 512KB
	Compression uint8         // 数据帧类型（protocol.TypeData=gzip / TypeDataZstd=zstd），默认 zstd
}

// 反压三级水位（与 influx-sync 同款）：绿 <60% 全速；黄 60%~80% 降速；红 ≥80% 挂起（迟滞）。
const (
	bpYellowTrigger = 0.60
	bpRedTrigger    = 0.80
	bpGreenResume   = 0.60
)

type bpState int

const (
	bpNormal bpState = iota
	bpDegraded
	bpPaused
)

// emptySkipMaxWindow 空窗自适应跳过的窗口上限：真空区 5s→10s→…→1h 翻倍，命中数据即复位。
const emptySkipMaxWindow = time.Hour

// Poller export 窗口轮询器：拉取 [cursor, now-watermark) 原始样本 → 分块 → WAL → 推进游标。
type Poller struct {
	client  *vm.Client
	wal     *wal.WAL
	metrics *monitor.Metrics
	logger  *zap.Logger
	cfg     PollerConfig

	emptyStreak int
}

// NewPoller 创建轮询器。
func NewPoller(client *vm.Client, w *wal.WAL, metrics *monitor.Metrics, logger *zap.Logger, cfg PollerConfig) *Poller {
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.Window <= 0 {
		cfg.Window = 5 * time.Second
	}
	if cfg.Watermark <= 0 {
		cfg.Watermark = time.Second
	}
	if cfg.MaxWindow <= 0 {
		cfg.MaxWindow = 30 * time.Second
	}
	if cfg.FrameLines <= 0 {
		cfg.FrameLines = 5000
	}
	if cfg.FrameBytes <= 0 {
		cfg.FrameBytes = 512 << 10
	}
	if cfg.Compression == 0 {
		cfg.Compression = protocol.TypeDataZstd
	}
	return &Poller{client: client, wal: w, metrics: metrics, logger: logger, cfg: cfg}
}

// Run 阻塞运行，直到 ctx 取消。
func (p *Poller) Run(ctx context.Context) {
	state := bpNormal
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	pausedStart := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		usage := WalDiskUsageRatio(p.wal.Dir())
		switch state {
		case bpNormal:
			if usage >= bpRedTrigger {
				state = bpPaused
				pausedStart = time.Now()
				p.logger.Warn("backpressure: poller paused", zap.Float64("disk_usage", usage))
			} else if usage >= bpYellowTrigger {
				state = bpDegraded
				p.logger.Warn("backpressure: degraded mode", zap.Float64("disk_usage", usage))
			}
		case bpDegraded:
			if usage >= bpRedTrigger {
				state = bpPaused
				pausedStart = time.Now()
				p.logger.Warn("backpressure: poller paused", zap.Float64("disk_usage", usage))
			} else if usage < bpGreenResume {
				state = bpNormal
			}
		case bpPaused:
			p.metrics.AddPausedSeconds(int64(time.Since(pausedStart).Seconds()))
			pausedStart = time.Now()
			if usage < bpGreenResume {
				state = bpNormal
				p.logger.Info("backpressure: poller resumed", zap.Float64("disk_usage", usage))
			}
		}
		p.metrics.SetBackpressureStatus(int64(state))
		p.metrics.SetWALDiskRatio(usage)

		switch state {
		case bpPaused:
			p.metrics.IncPollSkip()
			continue
		case bpDegraded:
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}

		p.pollOnce(ctx)
	}
}

// WalDiskUsageRatio 返回指定目录挂载盘占用率（0~1）；统计失败返回 0（不干预）。
func WalDiskUsageRatio(dir string) float64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0
	}
	total := st.Blocks * uint64(st.Bsize)
	avail := st.Bavail * uint64(st.Bsize)
	if total == 0 {
		return 0
	}
	return float64(total-avail) / float64(total)
}

// windowSize 空窗翻倍（上限 1h）：快速越过"回拨边界早于库内最早数据"的真空区。
func (p *Poller) windowSize() time.Duration {
	w := p.cfg.Window
	for i := 0; i < p.emptyStreak && w < emptySkipMaxWindow; i++ {
		w *= 2
	}
	if w > emptySkipMaxWindow {
		w = emptySkipMaxWindow
	}
	return w
}

// pollOnce 执行一轮 export 查询 → 分块 → WAL → 推进游标（先 WAL 后游标铁律）。
func (p *Poller) pollOnce(ctx context.Context) {
	now := time.Now().UnixMilli()
	cursor := p.wal.Cursor() // 毫秒
	window := p.windowSize()
	end := cursor + window.Milliseconds()
	if maxEnd := now - p.cfg.Watermark.Milliseconds(); end > maxEnd {
		end = maxEnd
	}
	if p.emptyStreak == 0 && end-cursor > p.cfg.MaxWindow.Milliseconds() {
		end = cursor + p.cfg.MaxWindow.Milliseconds()
	}
	if end <= cursor {
		return // 无新窗口
	}

	raw, err := p.client.ExportRange(ctx, cursor, end)
	if err != nil {
		p.logger.Warn("export failed, keep cursor", zap.Error(err))
		return // 保持游标，下轮重试
	}
	frames := splitFrames(raw, p.cfg.FrameLines, p.cfg.FrameBytes)
	if len(frames) == 0 {
		p.emptyStreak++
	} else {
		p.emptyStreak = 0
	}
	if len(frames) == 0 {
		// 空窗口：仍推进游标（该区间确实无数据）
		if err := p.wal.SetCursor(end); err != nil {
			p.logger.Error("cursor update failed", zap.Error(err))
			return
		}
		p.metrics.SetCursor(end)
		return
	}
	p.logger.Info("poll window",
		zap.Int64("start", cursor), zap.Int64("end", end),
		zap.Int("frames", len(frames)), zap.Int("bytes", len(raw)))

	// 分块编码（gzip/zstd，seq 占位 0——真实 seq 由 WAL.AppendBatch 锁内分配）
	enc := make([][]byte, 0, len(frames))
	for _, f := range frames {
		fb, err := protocol.Encode(p.cfg.Compression, 0, f)
		if err != nil {
			p.logger.Error("frame encode failed, keep cursor", zap.Error(err))
			return
		}
		enc = append(enc, fb)
	}
	// 先写 WAL，成功后才推进游标（顺序铁律，违反会漏数据）
	if _, err := p.wal.AppendBatch(p.cfg.Compression, enc); err != nil {
		p.logger.Error("wal append failed, keep cursor", zap.Error(err))
		return
	}
	if err := p.wal.SetCursor(end); err != nil {
		p.logger.Error("cursor update failed", zap.Error(err))
		return
	}
	p.metrics.SetCursor(end)
	p.metrics.SetWALPending(int64(p.wal.PendingCount()))
	p.metrics.SetWALBytes(p.wal.DiskUsage())
}

// splitFrames 将 export JSON lines 按"行数 + 字节"双阈值分块；单行绝不拆分。
func splitFrames(raw []byte, maxLines, maxBytes int) [][]byte {
	var frames [][]byte
	var cur []byte
	curLines := 0
	flush := func() {
		if len(cur) > 0 {
			frames = append(frames, cur)
			cur = nil
			curLines = 0
		}
	}
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		if curLines > 0 && (curLines >= maxLines || len(cur)+len(line)+1 > maxBytes) {
			flush()
		}
		if curLines > 0 {
			cur = append(cur, '\n')
		}
		cur = append(cur, line...)
		curLines++
		if len(cur) > maxBytes {
			// 单行超限：整行成帧（接收端 import 可处理任意大小行）
			flush()
		}
	}
	flush()
	return frames
}
