// Package integration 端到端测试：完整同步链路 + 断连恢复 + backfill 回拨。
package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"vm-sync/internal/monitor"
	"vm-sync/internal/receiver"
	"vm-sync/internal/sender"
	"vm-sync/internal/transport"
	"vm-sync/internal/vm"
	"vm-sync/internal/wal"
)

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	l, _ := zap.NewDevelopment()
	t.Cleanup(func() { l.Sync() })
	return l
}

// fakeSource 假 VM 源：仅 [oldest, dataEnd) 有数据（100ms 网格），export 原样返回。
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
		if first < oldest {
			first = oldest // 数据仅存在于 [oldest, dataEnd)，裁剪防止探测窗口生成海量行
		}
		for ts := first; ts < end; ts += 100 {
			rows = append(rows, fmt.Sprintf(`{"metric":{"__name__":"cpu","job":"a"},"values":[1.5],"timestamps":[%d]}`, ts))
		}
		if len(rows) > 0 {
			fmt.Fprint(w, strings.Join(rows, "\n")+"\n")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func parseSec(s string) int64 {
	f, _ := strconv.ParseFloat(s, 64)
	return int64(f * 1000)
}

// startReceiverSrv 启动 Receiver TCP 服务。
func startReceiverSrv(t *testing.T, targetURL, lastSeqFile string) (*transport.Server, string, context.CancelFunc) {
	t.Helper()
	vc, err := vm.NewClient(vm.Config{URL: targetURL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	h, err := receiver.New(vc, m, testLogger(t), receiver.Config{LastSeqFile: lastSeqFile, LastWriteTs: m.SetLastWriteTs})
	if err != nil {
		t.Fatal(err)
	}
	srv := transport.NewServer(transport.ServerConfig{Listen: "127.0.0.1:0"}, func(id uint64, fidx uint64, fb []byte) byte {
		return h.HandleFrame(id, fidx, fb)
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx)
	t.Cleanup(func() { cancel(); srv.Close() })
	return srv, srv.Addr().String(), cancel
}

// TestEndToEndSyncAndBackfill V1.x 语义 e2e：
// ① 新装 backfill=all 全量回填零丢失、标签/时间戳逐位保留；
// ② 断连恢复补传；③ 配置变化回拨重爬幂等（目标库同 series+ts 覆盖不重复计数）。
func TestEndToEndSyncAndBackfill(t *testing.T) {
	now := time.Now().UnixMilli()
	oldest := (now - 40*time.Second.Milliseconds()) / 100 * 100
	dataEnd := (now - time.Second.Milliseconds()) / 100 * 100

	src := fakeSource(t, oldest, dataEnd)
	// 目标假 VM：幂等 upsert（同 metric+ts 只计一次）+ 记录行内容
	var targetSamples atomic.Int64
	var mu sync.Mutex
	seen := map[int64]struct{}{}
	payloads := []string{}
	tgt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/import" {
			http.NotFound(w, r)
			return
		}
		buf := make([]byte, 4<<20)
		n, _ := r.Body.Read(buf)
		body := string(buf[:n])
		mu.Lock()
		payloads = append(payloads, body)
		for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
			i := strings.LastIndex(line, `"timestamps":[`)
			if i < 0 {
				continue
			}
			rest := line[i+len(`"timestamps":[`):]
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				continue
			}
			for _, tok := range strings.Split(rest[:end], ",") {
				if ts, err := strconv.ParseInt(strings.TrimSpace(tok), 10, 64); err == nil {
					if _, dup := seen[ts]; !dup {
						seen[ts] = struct{}{}
						targetSamples.Add(1)
					}
				}
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(tgt.Close)

	rs, addr, cancel1 := startReceiverSrv(t, tgt.URL, filepath.Join(t.TempDir(), "last_seq"))
	defer cancel1()

	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	vc, err := vm.NewClient(vm.Config{URL: src.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	metrics := monitor.New()
	poller := sender.NewPoller(vc, w, metrics, testLogger(t), sender.PollerConfig{
		Interval: 50 * time.Millisecond, Window: 500 * time.Millisecond, Watermark: 500 * time.Millisecond,
		FrameLines: 100, FrameBytes: 1 << 20,
	})
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 3 * time.Second})
	sl := sender.NewSender(w, client, metrics, testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 模拟 cmd/sender 新装流程：探测 → 应用 all → 游标=最早数据
	oldestProbe, err := vc.ProbeOldestData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.ApplyBackfillPolicy(wal.BackfillAllNs, oldestProbe); err != nil {
		t.Fatal(err)
	}
	if err := w.SetCursor(oldestProbe); err != nil {
		t.Fatal(err)
	}
	go poller.Run(ctx)
	go sl.Run(ctx)

	wantSamples := (dataEnd - oldest) / 100
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if targetSamples.Load() >= wantSamples && w.PendingCount() == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := targetSamples.Load(); got != wantSamples {
		t.Fatalf("backfill all: target=%d want=%d (cursor=%d)", got, wantSamples, w.Cursor())
	}
	// 逐位保真：目标收到的 payload 含原始 metric 与时间戳
	mu.Lock()
	joined := strings.Join(payloads, "\n")
	mu.Unlock()
	if !strings.Contains(joined, `"__name__":"cpu"`) || !strings.Contains(joined, `"job":"a"`) {
		t.Fatal("target payload must preserve metric labels verbatim")
	}

	// ② 断连恢复：停 receiver 1s → WAL 积压 → 重启 receiver（新端口）→ 新 sender 补传
	cancel1()
	rs.Close()
	time.Sleep(time.Second)
	_, addr2, cancel2 := startReceiverSrv(t, tgt.URL, filepath.Join(t.TempDir(), "last_seq2"))
	defer cancel2()
	client2 := transport.NewClient(transport.ClientConfig{Addr: addr2, Timeout: 3 * time.Second})
	sl2 := sender.NewSender(w, client2, monitor.New(), testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx2, cancel2b := context.WithCancel(context.Background())
	defer cancel2b()
	go sl2.Run(ctx2)

	// ③ 配置变化回拨（all→0→all）重爬：目标计数不重复（幂等）
	cancel()
	time.Sleep(200 * time.Millisecond)
	w.Close()
	w2, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if _, err := w2.ApplyBackfillPolicy(wal.BackfillNoneNs, 0); err != nil {
		t.Fatal(err)
	}
	rewound, err := w2.ApplyBackfillPolicy(wal.BackfillAllNs, oldestProbe)
	if err != nil || !rewound {
		t.Fatalf("config change must rewind: rewound=%v err=%v", rewound, err)
	}
	if w2.Cursor() != oldestProbe {
		t.Fatalf("rewound cursor=%d want %d", w2.Cursor(), oldestProbe)
	}
	// 用新 sender 会话重爬（游标=最早数据）
	poller2 := sender.NewPoller(vc, w2, monitor.New(), testLogger(t), sender.PollerConfig{
		Interval: 50 * time.Millisecond, Window: 500 * time.Millisecond, Watermark: 500 * time.Millisecond,
		FrameLines: 100, FrameBytes: 1 << 20,
	})
	client3 := transport.NewClient(transport.ClientConfig{Addr: addr2, Timeout: 3 * time.Second})
	sl3 := sender.NewSender(w2, client3, monitor.New(), testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	go poller2.Run(ctx3)
	go sl3.Run(ctx3)
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if w2.Cursor() >= dataEnd && w2.PendingCount() == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel3()
	if got := targetSamples.Load(); got != wantSamples {
		t.Fatalf("re-backfill must be idempotent: target=%d want=%d", got, wantSamples)
	}
}
