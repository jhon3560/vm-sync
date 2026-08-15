package sender

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
	dataEnd := (now - time.Second.Milliseconds()) / 100 * 100
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
	// 所有样本都已进 WAL（帧未发送，pending=帧数）：每 0.5s 窗口一帧（每窗口 5 行 < FrameLines）
	pending := w.PendingCount()
	wantFrames := (dataEnd - oldest) / 500
	if pending == 0 || pending < int(wantFrames)-2 || pending > int(wantFrames)+2 {
		t.Fatalf("pending=%d want≈%d", pending, wantFrames)
	}
}
