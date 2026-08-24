package sender

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"vm-sync/internal/monitor"
	"vm-sync/internal/vm"
	"vm-sync/internal/wal"
)

func TestSplitFrames(t *testing.T) {
	mkLines := func(n int) []byte {
		var b bytes.Buffer
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, `{"metric":{"__name__":"m%d"},"values":[1],"timestamps":[%d]}`+"\n", i, i)
		}
		return b.Bytes()
	}
	// 按行数分块：10 行 / 每帧 4 行 → 4+4+2 = 3 帧（行间换行，无尾随换行）
	frames := splitFrames(mkLines(10), 4, 1<<20)
	if len(frames) != 3 || bytes.Count(frames[0], []byte{'\n'}) != 3 {
		t.Fatalf("line-split: %d frames (first frame newlines=%d)", len(frames), bytes.Count(frames[0], []byte{'\n'}))
	}
	// 按字节分块（单行 60B，上限 200B → 3 行一帧）
	frames = splitFrames(mkLines(10), 1000, 200)
	if len(frames) < 3 {
		t.Fatalf("byte-split: %d frames", len(frames))
	}
	// 单行超大：整行成帧不拆分
	big := []byte(strings.Repeat("x", 5000))
	frames = splitFrames(big, 100, 100)
	if len(frames) != 1 || !bytes.Equal(frames[0], big) {
		t.Fatalf("oversized line: %d frames", len(frames))
	}
	// 空输入
	if frames := splitFrames(nil, 10, 10); len(frames) != 0 {
		t.Fatalf("empty: %d frames", len(frames))
	}
}

// fakeSource 可控数据的假 VM 源：仅 [oldest, dataEnd) 有数据，网格 100ms。
func fakeSource(t *testing.T, oldest, dataEnd int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		start := parseSec(r.URL.Query().Get("start"))
		end := parseSec(r.URL.Query().Get("end"))
		if end > dataEnd {
			end = dataEnd
		}
		var rows []string
		first := (start + 99) / 100 * 100
		for ts := first; ts < end; ts += 100 {
			rows = append(rows, fmt.Sprintf(`{"metric":{"__name__":"cpu","job":"a"},"values":[1.5],"timestamps":[%d]}`, ts))
		}
		fmt.Fprint(w, strings.Join(rows, "\n"))
		if len(rows) > 0 {
			fmt.Fprint(w, "\n")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func parseSec(s string) int64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return int64(f * 1000)
}

func TestPollerBackfillAllZeroLoss(t *testing.T) {
	now := time.Now().UnixMilli()
	oldest := (now - 40*time.Second.Milliseconds()) / 100 * 100
	// R24：假源不支持 /internal/force_flush → 实时区（距 now < 15s）回退保守
	// 收口，游标合法停在 now-15s——数据端必须取在实时区之前（now-17s）。
	dataEnd := (now - 17*time.Second.Milliseconds()) / 100 * 100
	src := fakeSource(t, oldest, dataEnd)

	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(oldest); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: src.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Interval: 20 * time.Millisecond, Window: 500 * time.Millisecond,
		Watermark: 500 * time.Millisecond, FrameLines: 100, FrameBytes: 1 << 20,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	// 等待游标追平到 dataEnd
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if w.Cursor() >= dataEnd {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if w.Cursor() < dataEnd {
		t.Fatalf("cursor=%d want>=%d", w.Cursor(), dataEnd)
	}
	// 所有样本都已进 WAL（帧未发送，pending=帧数）。N14 起稀疏数据（每窗 5 行
	// < 增长目标 400 行）窗口翻倍到 MaxWindow(30s) 封顶 → 帧数远少于逐 0.5s 窗
	// （~9 帧 vs ~78 帧），但游标追平 ⟹ 数据全量在 WAL（先 WAL 后游标铁律）。
	pending := w.PendingCount()
	wantFrames := (dataEnd - oldest) / 500
	if pending == 0 || pending > int(wantFrames) {
		t.Fatalf("pending=%d want in [1, %d]", pending, wantFrames)
	}
}

// sparseSource 稀疏数据假 VM 源：仅每 10s 一行，数据区 [0, 6h)。
func sparseSource(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		start := parseSec(r.URL.Query().Get("start"))
		end := parseSec(r.URL.Query().Get("end"))
		if end > int64(6*time.Hour/time.Millisecond) {
			end = int64(6 * time.Hour / time.Millisecond)
		}
		var rows []string
		first := (start + 9999) / 10000 * 10000
		for ts := first; ts < end; ts += 10000 {
			rows = append(rows, fmt.Sprintf(`{"metric":{"__name__":"sparse"},"values":[1],"timestamps":[%d]}`, ts))
		}
		fmt.Fprint(w, strings.Join(rows, "\n"))
		if len(rows) > 0 {
			fmt.Fprint(w, "\n")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestPollerSparseWindowGrowth N14 回归：稀疏数据（行数 << 增长目标）的窗口必须
// 与空窗一样翻倍（封顶 MaxWindow）——修复前稀疏库回填被基础窗口封顶
// （influx-sync 实测稀疏库修复前需十几天）。
func TestPollerSparseWindowGrowth(t *testing.T) {
	srv := sparseSource(t)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 30 * time.Second,
		FrameLines: 10, WindowTarget: 10000, // 字节阈值：~70B/行稀疏数据恒欠满 → 翻倍到 MaxWindow
	})
	// 稀疏 0.1 行/s（~7B/行）：每窗字节数 < 10000 目标 → 翻倍至 MaxWindow(30s) 封顶。
	// 12 轮后游标应远超 12×5s=60s（修复前约 60s）。
	for i := 0; i < 12; i++ {
		p.pollOnce(context.Background())
	}
	if w.Cursor() < int64(300*time.Second/time.Millisecond) {
		t.Fatalf("cursor=%d want >= %d (sparse window must grow to MaxWindow)", w.Cursor(), int64(300*time.Second/time.Millisecond))
	}
}

// TestPollerPrefetchPipeline N16 回归：单窗口轮次的下一窗口 export 在处理本轮时
// 已在途；多轮后游标正确推进、数据全量入 WAL。
func TestPollerPrefetchPipeline(t *testing.T) {
	var mu sync.Mutex
	var queryCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		queryCount++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond) // 模拟 export 延迟
		start := parseSec(r.URL.Query().Get("start"))
		end := parseSec(r.URL.Query().Get("end"))
		var rows []string
		for ts := start + 100; ts < end; ts += 250 { // 4 行/s，5s 窗 = 20 行
			rows = append(rows, fmt.Sprintf(`{"metric":{"__name__":"dense"},"values":[1],"timestamps":[%d]}`, ts))
		}
		fmt.Fprint(w, strings.Join(rows, "\n"))
		if len(rows) > 0 {
			fmt.Fprint(w, "\n")
		}
	}))
	t.Cleanup(srv.Close)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 5 * time.Second,
		FrameLines: 1000, WindowTarget: 10, // 每窗 20 行 ≥ 10 → streak 恒 0，窗口恒 5s（预取路径）
	})
	const rounds = 6
	for i := 0; i < rounds; i++ {
		p.pollOnce(context.Background())
	}
	if w.Cursor() != int64(rounds*5*time.Second/time.Millisecond) {
		t.Fatalf("cursor=%d want %d", w.Cursor(), int64(rounds*5*time.Second/time.Millisecond))
	}
	if w.PendingCount() == 0 {
		t.Fatal("data must land in wal")
	}
	mu.Lock()
	qc := queryCount
	mu.Unlock()
	// 预取生效：6 轮 + 最多 1 个在途预取 = 7 次 export；无预取恒等于轮数。
	if qc > rounds+1 {
		t.Fatalf("query count=%d want <= %d (prefetch storms?)", qc, rounds+1)
	}
}

// TestPollerPrefetchDiscardOnMismatch N16 防御：预取槽游标与当前游标不符
// （上一轮处理失败未推进）→ 丢弃并同步重查，不得跳过窗口（零丢失）。
func TestPollerPrefetchDiscardOnMismatch(t *testing.T) {
	var queryCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&queryCount, 1)
		start := parseSec(r.URL.Query().Get("start"))
		end := parseSec(r.URL.Query().Get("end"))
		var rows []string
		for ts := start + 100; ts < end; ts += 500 {
			rows = append(rows, fmt.Sprintf(`{"metric":{"__name__":"m"},"values":[1],"timestamps":[%d]}`, ts))
		}
		fmt.Fprint(w, strings.Join(rows, "\n"))
		if len(rows) > 0 {
			fmt.Fprint(w, "\n")
		}
	}))
	t.Cleanup(srv.Close)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 5 * time.Second,
		FrameLines: 1000, WindowTarget: 1000,
	})
	// 注入一个游标不符的预取槽（模拟上一轮失败后残留）
	p.prefetch = &prefetchSlot{cursor: int64(99 * 1000), end: int64(104 * 1000), ch: make(chan prefetchResult, 1)}
	p.prefetch.ch <- prefetchResult{raw: nil, err: nil}
	qBefore := atomic.LoadInt32(&queryCount)
	p.pollOnce(context.Background())
	// 失配 → 同步重查 [0,5s)（+1），随后下一窗口预取（+1）——断言重查发生
	if atomic.LoadInt32(&queryCount) < qBefore+1 {
		t.Fatalf("query count=%d want >= %d (mismatch must re-export)", atomic.LoadInt32(&queryCount), qBefore+1)
	}
	if w.Cursor() != int64(5*time.Second/time.Millisecond) {
		t.Fatalf("cursor=%d want %d", w.Cursor(), int64(5*time.Second/time.Millisecond))
	}
}

// TestPollerExportLagNormalization R24 回归：有效导出可见性余量
// = max(watermark, exportLag, 15s)——VM export 看不到 pending rows（≈4s），
// 新序列名更要等索引 flushCallback 10s 节拍（≈10.5s）才可被查到；余量低于
// 15s 会让游标越过不可见数据造成永久漏发。
func TestPollerExportLagNormalization(t *testing.T) {
	cases := []struct {
		name      string
		watermark time.Duration
		exportLag time.Duration
		wantLag   time.Duration
		wantWM    time.Duration
	}{
		{"default", time.Second, 0, DefaultExportVisibilityLag, time.Second},
		{"zero watermark", 0, 0, DefaultExportVisibilityLag, time.Second},
		{"small explicit lag still floored", time.Second, 3 * time.Second, DefaultExportVisibilityLag, time.Second},
		{"explicit 10s lag still floored (new-series 10.5s)", time.Second, 10 * time.Second, DefaultExportVisibilityLag, time.Second},
		{"explicit large lag", time.Second, 20 * time.Second, 20 * time.Second, time.Second},
		{"watermark dominates", 30 * time.Second, 20 * time.Second, 30 * time.Second, 30 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := NewPoller(nil, nil, nil, nil, PollerConfig{Watermark: c.watermark, ExportLag: c.exportLag})
			if p.cfg.ExportLag != c.wantLag {
				t.Fatalf("exportLag=%v want %v", p.cfg.ExportLag, c.wantLag)
			}
			if p.cfg.Watermark != c.wantWM {
				t.Fatalf("watermark=%v want %v", p.cfg.Watermark, c.wantWM)
			}
		})
	}
}

// TestPollerWindowEndExportLagClamp R24 回归：窗口尾端 = min(cursor+window,
// now-收口余量)；实时区 flush 成功按 watermark 收口，失败回退 ExportLag；
// 非增长态下另受 MaxWindow 钳制（R6）。
func TestPollerWindowEndExportLagClamp(t *testing.T) {
	p := NewPoller(nil, nil, nil, nil, PollerConfig{
		Watermark: time.Second, Window: 5 * time.Second, MaxWindow: 30 * time.Second,
	})
	now := time.Now().UnixMilli()
	lagMs := p.cfg.ExportLag.Milliseconds()
	// 回填态（cursor 远离 now）：不受可见性收口影响，end = cursor+window
	cursor := now - int64(time.Hour/time.Millisecond)
	if got, want := p.windowEnd(cursor, now, true), cursor+5000; got != want {
		t.Fatalf("backfill: end=%d want %d", got, want)
	}
	// 实时态 + flush 成功：end 钳到 now-watermark
	cursor = now - int64(6*time.Second/time.Millisecond)
	if got, want := p.windowEnd(cursor, now, true), now-1000; got != want {
		t.Fatalf("realtime flushOK: end=%d want %d", got, want)
	}
	// 实时态 + flush 失败：end 钳到 now-ExportLag（保守回退）
	cursor = now - int64(6*time.Second/time.Millisecond)
	if got, want := p.windowEnd(cursor, now, false), now-lagMs; got != want {
		t.Fatalf("realtime flushFail: end=%d want %d", got, want)
	}
	// MaxWindow 钳位（非增长态 + Window 超 MaxWindow 的误配置，R6）
	p2 := NewPoller(nil, nil, nil, nil, PollerConfig{
		Watermark: time.Second, Window: time.Minute, MaxWindow: 10 * time.Second,
	})
	cursor = now - int64(time.Hour/time.Millisecond)
	if got, want := p2.windowEnd(cursor, now, true), cursor+10000; got != want {
		t.Fatalf("maxwindow: end=%d want %d", got, want)
	}
}

// TestPollerRealtimeDoesNotPassVisibilityLag R24 回归（行为级）：实时区 flush
// 失败时窗口尾端回退到 now-ExportLag——修复前窗口尾端=now-watermark(1s)，游标
// 在数据可见（pending rows ≈4s / 新序列 ≈10.5s）前越过其时间戳，实时写入
// 永久漏发（e2e 实测默认配置下实时写入 100% 丢失）。假源不支持 /internal/
// force_flush（404）→ flush 必然失败 → 游标不得越过 now-15s。
func TestPollerRealtimeDoesNotPassVisibilityLag(t *testing.T) {
	now := time.Now().UnixMilli()
	oldest := (now - 2*time.Minute.Milliseconds()) / 100 * 100
	srv := fakeSource(t, oldest, now) // 数据一直写到 now（理想化源，但无 force_flush）
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(oldest); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Interval: 20 * time.Millisecond, Window: 5 * time.Second, Watermark: time.Second,
		FrameLines: 100000, FrameBytes: 1 << 20,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	// 游标应推进到保守余量附近（now-16s 以内，可见区数据已入 WAL）
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if w.Cursor() >= now-int64(16*time.Second/time.Millisecond) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	nowCheck := time.Now().UnixMilli()
	if w.Cursor() < now-int64(16*time.Second/time.Millisecond) {
		t.Fatalf("cursor=%d must advance to near now-lag", w.Cursor())
	}
	// 红线：flush 失败时游标任何时候都不得越过 now-15s（用 cancel 后的
	// nowCheck 兑底 pollOnce 内部 now 略早于 nowCheck）
	if w.Cursor() > nowCheck-p.cfg.ExportLag.Milliseconds() {
		t.Fatalf("cursor=%d must not pass now-lag=%d", w.Cursor(), nowCheck-p.cfg.ExportLag.Milliseconds())
	}
	if w.PendingCount() == 0 {
		t.Fatal("visible-zone data must land in wal")
	}
}

// TestPollerRealtimeForceFlushTightWatermark R24 对照：实时区 force_flush 成功时
// 窗口尾端按 watermark 收口（实时 e2e ≈1.5~2.5s 的语义恢复）——游标必须能推进
// 到 now-1s 附近而不是停在 now-15s。
func TestPollerRealtimeForceFlushTightWatermark(t *testing.T) {
	now := time.Now().UnixMilli()
	oldest := (now - 30*time.Second.Milliseconds()) / 100 * 100
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/force_flush" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		start := parseSec(r.URL.Query().Get("start"))
		end := parseSec(r.URL.Query().Get("end"))
		if end > now {
			end = now
		}
		var rows []string
		first := (start + 99) / 100 * 100
		for ts := first; ts < end; ts += 100 {
			rows = append(rows, fmt.Sprintf(`{"metric":{"__name__":"cpu","job":"a"},"values":[1.5],"timestamps":[%d]}`, ts))
		}
		fmt.Fprint(w, strings.Join(rows, "\n"))
		if len(rows) > 0 {
			fmt.Fprint(w, "\n")
		}
	}))
	t.Cleanup(srv.Close)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(oldest); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Interval: 20 * time.Millisecond, Window: 5 * time.Second, Watermark: time.Second,
		FrameLines: 100000, FrameBytes: 1 << 20,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	// flush 成功 → 游标必须能越过 now-15s、追平到 now-2s 以内
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if w.Cursor() >= now-int64(2*time.Second/time.Millisecond) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if w.Cursor() < now-int64(2*time.Second/time.Millisecond) {
		t.Fatalf("cursor=%d must reach near now-watermark when flush works", w.Cursor())
	}
	if w.PendingCount() == 0 {
		t.Fatal("data must land in wal")
	}
}
// TestPollerExportErrorResetsStreak N15 回归：export 失败复位窗口增长——
// 下轮回基础窗口自愈，避免大窗口持续失败停滞。
func TestPollerExportErrorResetsStreak(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		start := parseSec(r.URL.Query().Get("start"))
		end := parseSec(r.URL.Query().Get("end"))
		if end-start > int64(10*time.Second/time.Millisecond) {
			http.Error(w, "window too large", http.StatusInternalServerError)
			return
		}
		var rows []string
		for ts := start + 100; ts < end; ts += 250 {
			rows = append(rows, fmt.Sprintf(`{"metric":{"__name__":"m"},"values":[1],"timestamps":[%d]}`, ts))
		}
		fmt.Fprint(w, strings.Join(rows, "\n"))
		if len(rows) > 0 {
			fmt.Fprint(w, "\n")
		}
	}))
	t.Cleanup(srv.Close)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 30 * time.Second,
		FrameLines: 1000, WindowTarget: 1000,
	})
	p.underfillStreak = 3 // 窗口已翻倍到 40s（> 假源 10s 上限）
	p.streakAllEmpty = false
	p.pollOnce(context.Background())
	if w.Cursor() != 0 {
		t.Fatalf("failed round must keep cursor, got %d", w.Cursor())
	}
	if p.underfillStreak != 0 || !p.streakAllEmpty {
		t.Fatalf("export failure must reset streak: streak=%d allEmpty=%v", p.underfillStreak, p.streakAllEmpty)
	}
	// 复位后基础窗口（5s ≤ 10s 上限）成功推进
	p.pollOnce(context.Background())
	if w.Cursor() != int64(5*time.Second/time.Millisecond) {
		t.Fatalf("cursor=%d want %d (self-heal with base window)", w.Cursor(), int64(5*time.Second/time.Millisecond))
	}
}

// TestPollerUnderfillByBytes R2 回归：欠满判定按响应**字节数**——少行数但大体积
// （单行多样本）的窗口必须判稠密（不复位则窗口翻倍到上限 → 周期触碰 N15 震荡）。
func TestPollerUnderfillByBytes(t *testing.T) {
	var big string
	{
		var b strings.Builder
		b.WriteString(`{"metric":{"__name__":"big"},"values":[`)
		for i := 0; i < 5000; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("1")
		}
		b.WriteString(`],"timestamps":[0,100,200]}` + "\n")
		big = b.String()
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		// 每窗固定 2 行 ≈ 12KB：行数极少（< FrameLines），但字节数大
		fmt.Fprint(w, big+big)
	}))
	t.Cleanup(srv.Close)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 30 * time.Second,
		FrameLines: 5000, WindowTarget: 5000, // 12KB/窗 ≥ 5KB → 稠密，窗口不翻倍
	})
	p.pollOnce(context.Background())
	if p.underfillStreak != 0 {
		t.Fatalf("dense-by-bytes window must reset streak, got %d", p.underfillStreak)
	}
	p.pollOnce(context.Background())
	if w.Cursor() != int64(10*time.Second/time.Millisecond) {
		t.Fatalf("cursor=%d want %d (no window growth for byte-dense data)", w.Cursor(), int64(10*time.Second/time.Millisecond))
	}
}

// TestPollerOversizeLineShrinksWindow R16 回归：单条 export 行（单序列样点过密）
// 无法成帧时，窗口逐轮减半直到单行可成帧——修复前 encode 持续失败、游标永不
// 推进（实测 3 轮后 cursor=0，每 500ms 重复拉取同一大窗，同步永久停滞）。
func TestPollerOversizeLineShrinksWindow(t *testing.T) {
	// 200 值/ms 的单序列密度；值使用固定字符串而非 rand——保持单行足够大
	// 的同时避免 race detector 下数百万次锁竞争/随机数生成拖慢到分钟级。
	mkLine := func(startMs, endMs int64) []byte {
		if endMs <= startMs {
			return nil
		}
		var b strings.Builder
		b.WriteString(`{"metric":{"__name__":"vib"},"values":[`)
		for i := int64(0); startMs+i/200 < endMs; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("123456789.123456789")
		}
		b.WriteString(`],"timestamps":[0]}` + "\n")
		return []byte(b.String())
	}
	// 数据仅存在于 [0, 6s)：请求窗口按实际数据裁剪，行大小随窗口成比例
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		start := parseSec(r.URL.Query().Get("start"))
		end := parseSec(r.URL.Query().Get("end"))
		if dataEnd := int64(6 * time.Second / time.Millisecond); end > dataEnd {
			end = dataEnd
		}
		w.Write(mkLine(start, end))
	}))
	t.Cleanup(srv.Close)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 10 * time.Second,
		FrameLines: 5000, WindowTarget: 5000,
	})
	// 5s 窗 = 1M 值 ≈ 20MB 原始：超 MaxDecompressedLen → encode 失败 → 收缩
	failedRounds := 0
	for i := 0; i < 20 && w.Cursor() == 0; i++ {
		p.pollOnce(context.Background())
		if w.Cursor() == 0 {
			failedRounds++
		}
	}
	if w.Cursor() == 0 {
		t.Fatal("cursor stuck at 0: oversized single line not handled by window shrink")
	}
	if failedRounds == 0 {
		t.Fatal("test setup: expected at least one oversize failure round")
	}
	t.Logf("shrunk after %d failed rounds, cursor=%d, oversizeStreak=%d",
		failedRounds, w.Cursor(), p.oversizeStreak)
	// 数据不得丢失：帧必须进入 WAL（先 WAL 后游标铁律）
	if w.PendingCount() == 0 {
		t.Fatal("frames must land in wal")
	}
	// 继续推进：收缩后的窗口持续成功，游标应继续前进
	c := w.Cursor()
	for i := 0; i < 5; i++ {
		p.pollOnce(context.Background())
	}
	if w.Cursor() <= c {
		t.Fatalf("cursor must keep advancing after shrink: %d -> %d", c, w.Cursor())
	}
}

// TestPollerPrefetchCtxCancel R1 配套：消费轮等待预取结果必须可被 ctx 取消打断
// （源 VM 挂起时预取查询可能 10s 级延迟，关停不得阻塞在其上）。
func TestPollerPrefetchCtxCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetCursor(0); err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	p := NewPoller(vc, w, m, zap.NewNop(), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 5 * time.Second,
		FrameLines: 1000, WindowTarget: 1000,
	})
	// 注入永不就绪的预取槽
	p.prefetch = &prefetchSlot{cursor: 0, end: 5000, ch: make(chan prefetchResult)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	p.pollOnce(ctx)
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("pollOnce must not block on prefetch after ctx cancel, blocked %v", el)
	}
}
