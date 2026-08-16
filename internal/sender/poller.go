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
	Interval     time.Duration // 轮询周期，默认 500ms
	Window       time.Duration // 查询窗口，默认 5s
	Watermark    time.Duration // 水位延迟，默认 1s（VM 写入即查询可见，仅防时钟抖动）
	MaxWindow    time.Duration // 单次查询窗口上限（防时间跳变），默认 30s
	FrameLines   int           // 每帧最多 export 行数（每行可含多样本），默认 5000
	FrameBytes   int           // 每帧压缩前字节上限，默认 512KB
	WindowTarget int           // N14/R2 窗口增长目标**字节数**（默认=FrameBytes×4）：欠满判定
	                          // 阈值，按 export 响应字节数判定（行数在高样本率少序列库会误判），
	                          // 与帧大小解耦——稀疏库窗口不被帧行数锁死
	Compression  uint8         // 数据帧类型（protocol.TypeData=gzip / TypeDataZstd=zstd），默认 zstd
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

	// underfillStreak 连续"欠满窗口"计数（N14）：空窗或行数 < 增长目标的稀疏窗
	// 都计入，驱动窗口翻倍——稀疏库回填不再被基础窗口封顶（influx-sync 实测
	// 稀疏库修复前需十几天，修复后受 MaxWindow 与查询时延约束）。
	// streakAllEmpty：streak 是否全由空窗构成——真空区翻倍上限 1h（响应为空安全）；
	// 出现稀疏窗后改为封顶 MaxWindow（响应有界）。
	underfillStreak int
	streakAllEmpty  bool

	// prefetch 单窗口轮次的下窗口预取（N16）：处理本轮结果时下一轮 export 在途，
	// 隐藏源库查询延迟（influx-sync 实测 ~1.8s/轮 → 0.41→0.76 天/分钟）。
	// 仅 Run 循环 goroutine 访问，无竞态。
	prefetch *prefetchSlot
}

// prefetchSlot 在途预取查询。consume 时若 cursor 与当前游标不符
// （上一轮处理失败游标未推进等），丢弃并同步重查——防御性兜底零丢失。
type prefetchSlot struct {
	cursor, end int64
	ch          chan prefetchResult
}

// prefetchResult 预取查询结果。
type prefetchResult struct {
	raw []byte
	err error
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
	if cfg.WindowTarget <= 0 {
		cfg.WindowTarget = cfg.FrameBytes * 4 // 默认按字节目标：4×帧字节
	}
	if cfg.Compression == 0 {
		cfg.Compression = protocol.TypeDataZstd
	}
	return &Poller{client: client, wal: w, metrics: metrics, logger: logger, cfg: cfg, streakAllEmpty: true}
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

// windowSize 欠满窗口翻倍（N14）：真空区（全空 streak）上限 1h，稀疏区封顶
// MaxWindow——快速越过"回拨边界早于库内最早数据"的真空区与稀疏数据区。
func (p *Poller) windowSize() time.Duration {
	cap := p.cfg.MaxWindow
	if p.streakAllEmpty {
		cap = emptySkipMaxWindow
	}
	w := p.cfg.Window
	for i := 0; i < p.underfillStreak && w < cap; i++ {
		w *= 2
	}
	if w > cap {
		w = cap
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
	if p.underfillStreak == 0 && end-cursor > p.cfg.MaxWindow.Milliseconds() {
		end = cursor + p.cfg.MaxWindow.Milliseconds()
	}
	if end <= cursor {
		return // 无新窗口
	}

	// N16：优先消费预取结果（处理本轮时上一轮已把 export 发出去）。
	// 消费轮直接采用槽的窗口边界（streak 每轮变化，重算必然失配）；
	// 仅当槽游标与当前游标不符（上一轮失败未推进）时丢弃同步重查——零丢失兜底。
	var raw []byte
	var err error
	if p.prefetch != nil {
		slot := p.prefetch
		p.prefetch = nil
		if slot.cursor == cursor {
			end = slot.end
			// R1：裸接收改为 ctx 可选等待——关停/取消时立即返回，不阻塞在
			// 预取查询上（源 VM 挂起时预取可能 10s 级延迟）；goroutine 结果
			// 写入 buffered channel（cap 1）后自然回收，无泄漏。
			select {
			case r := <-slot.ch:
				raw, err = r.raw, r.err
			case <-ctx.Done():
				return
			}
		} else {
			p.logger.Debug("prefetch discarded (cursor mismatch), re-export",
				zap.Int64("slot_cursor", slot.cursor), zap.Int64("cursor", cursor))
			raw, err = p.client.ExportRange(ctx, cursor, end)
		}
	} else {
		raw, err = p.client.ExportRange(ctx, cursor, end)
	}
	if err != nil {
		p.logger.Warn("export failed, keep cursor", zap.Error(err))
		// N15：export 失败复位窗口增长——下轮回基础窗口（小窗口更易成功），
		// 避免大窗口持续失败导致停滞（如响应超限/源库抖动）。
		p.underfillStreak = 0
		p.streakAllEmpty = true
		return // 保持游标，下轮重试
	}

	// N16：立即启动下一窗口预取（与下方分块/编码/WAL 并行，隐藏 export 延迟）。
	// 放在欠满判定之后：窗口估算用刚观测到的密度（streak 已更新）。
	frames := splitFrames(raw, p.cfg.FrameLines, p.cfg.FrameBytes)
	// N14/R2：欠满计数按**响应字节数**判定（空窗或字节数 < 增长目标的稀疏窗
	// 均翻倍跳过）——不用行数：VM export 单行可含大量样本，少序列×高样本率库
	// 行数恒小会被误判稀疏 → 窗口翻倍到上限 → 周期触碰 N15 超限震荡。
	// 命中稠密数据（≥ 增长目标字节）复位。
	switch {
	case len(frames) == 0:
		p.underfillStreak++
		// streakAllEmpty 保持（空窗不改变"全空"性质）
	case len(raw) < p.cfg.WindowTarget:
		p.underfillStreak++
		p.streakAllEmpty = false
	default:
		p.underfillStreak = 0
		p.streakAllEmpty = true
	}
	p.launchPrefetch(ctx, end)
	if len(frames) == 0 {
		// 空窗口：仍推进游标（该区间确实无数据）
		if err := p.wal.SetCursor(end); err != nil {
			p.logger.Error("cursor update failed", zap.Error(err))
			p.prefetch = nil // 游标未推进，预取结果作废（consume 时兜底再查）
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
			p.prefetch = nil
			return
		}
		enc = append(enc, fb)
	}
	// 先写 WAL，成功后才推进游标（顺序铁律，违反会漏数据）
	if _, err := p.wal.AppendBatch(p.cfg.Compression, enc); err != nil {
		p.logger.Error("wal append failed, keep cursor", zap.Error(err))
		p.prefetch = nil
		return
	}
	if err := p.wal.SetCursor(end); err != nil {
		p.logger.Error("cursor update failed", zap.Error(err))
		p.prefetch = nil
		return
	}
	p.metrics.SetCursor(end)
	p.metrics.SetWALPending(int64(p.wal.PendingCount()))
	p.metrics.SetWALBytes(p.wal.DiskUsage())
}

// launchPrefetch 启动 [cursor, cursor+window) 的预取 export（N16）。窗口按当前
// streak 估算（真空/稀疏规则同 windowSize）；仅 export 不推进游标。export 失败
// 由消费轮统一处理（复位增长 + 同步重查），不会丢窗口。
func (p *Poller) launchPrefetch(ctx context.Context, cursor int64) {
	if p.prefetch != nil {
		return // 已有预取在途
	}
	nw := p.windowSize()
	// R6：与 pollOnce 同款守卫——非增长态下窗口不得越过 MaxWindow
	// （防误配置 Window > MaxWindow 时预取窗口与轮询窗口不一致）。
	if p.underfillStreak == 0 && nw > p.cfg.MaxWindow {
		nw = p.cfg.MaxWindow
	}
	ne := cursor + nw.Milliseconds()
	if maxEnd := time.Now().UnixMilli() - p.cfg.Watermark.Milliseconds(); ne > maxEnd {
		ne = maxEnd
	}
	if ne <= cursor {
		return // 已追平水位，无窗口可预取
	}
	ch := make(chan prefetchResult, 1)
	go func() {
		raw, err := p.client.ExportRange(ctx, cursor, ne)
		ch <- prefetchResult{raw: raw, err: err}
	}()
	p.prefetch = &prefetchSlot{cursor: cursor, end: ne, ch: ch}
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
