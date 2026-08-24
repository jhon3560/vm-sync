package receiver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"

	"vm-sync/internal/monitor"
	"vm-sync/internal/protocol"
	"vm-sync/internal/vm"
)

func newTestReceiver(t *testing.T, srv *httptest.Server, cfg Config) *Receiver {
	t.Helper()
	vc, err := vm.NewClient(vm.Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(vc, monitor.New(), zap.NewNop(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// fakeTarget 假目标 VM：统计 import 次数与内容，可配置失败。
func fakeTarget(t *testing.T, fail bool) (*httptest.Server, *atomic.Int64, *sync.Map) {
	t.Helper()
	var writes atomic.Int64
	written := &sync.Map{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/import" {
			http.NotFound(w, r)
			return
		}
		writes.Add(1)
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		buf := make([]byte, 1<<20)
		n, _ := r.Body.Read(buf)
		written.Store("last", string(buf[:n]))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, &writes, written
}

func TestHandleDataFrame(t *testing.T) {
	srv, writes, written := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	payload := `{"metric":{"__name__":"cpu"},"values":[1.5],"timestamps":[1786800000000]}` + "\n"
	fb, _ := protocol.EncodeDataZstd(1, []byte(payload))
	if ack := r.HandleFrame(1, 0, fb); ack != protocol.AckSuccess {
		t.Fatalf("ack=%x", ack)
	}
	if writes.Load() != 1 {
		t.Fatalf("writes=%d", writes.Load())
	}
	v, _ := written.Load("last")
	if !strings.Contains(v.(string), `"__name__":"cpu"`) {
		t.Fatalf("import body mismatch: %q", v)
	}
	if r.LastSeq() != 1 {
		t.Fatalf("last_seq=%d", r.LastSeq())
	}
}

func TestTransientFailureRetryNotSwallowed(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var writes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/import" {
			http.NotFound(w, r)
			return
		}
		writes.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeDataZstd(401, []byte(`{"metric":{"__name__":"c"},"values":[1],"timestamps":[1]}`+"\n"))
	if ack := r.HandleFrame(1, 0, fb); ack != protocol.AckFail {
		t.Fatalf("first ack=%x, want 0x00", ack)
	}
	fail.Store(false)
	if ack := r.HandleFrame(1, 1, fb); ack != protocol.AckSuccess {
		t.Fatalf("retry ack=%x, want 0xff", ack)
	}
	if writes.Load() != 2 {
		t.Fatalf("retry must actually write (writes=%d)", writes.Load())
	}
}

func TestPoisonIsolatedToDLQ(t *testing.T) {
	dlqDir := filepath.Join(t.TempDir(), "dlq")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "bad")
	}))
	defer srv.Close()
	r := newTestReceiver(t, srv, Config{DLQDir: dlqDir})
	fb, _ := protocol.EncodeDataZstd(7, []byte(`{"metric":{"__name__":"bad"},"values":[1],"timestamps":[1]}`+"\n"))
	if ack := r.HandleFrame(1, 0, fb); ack != protocol.AckSuccess {
		t.Fatalf("poison must ack 0xff (unblock link), got %x", ack)
	}
	entries, _ := os.ReadDir(dlqDir)
	if len(entries) != 1 {
		t.Fatalf("dlq files=%d", len(entries))
	}
	if r.LastSeq() != 7 {
		t.Fatalf("last_seq=%d", r.LastSeq())
	}
}

func TestDuplicateSeqDedup(t *testing.T) {
	srv, writes, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeDataZstd(5, []byte(`{"metric":{"__name__":"c"},"values":[1],"timestamps":[1]}`+"\n"))
	if ack := r.HandleFrame(1, 0, fb); ack != protocol.AckSuccess {
		t.Fatalf("ack=%x", ack)
	}
	// 重发同 seq：直接 0xff，不重复写
	if ack := r.HandleFrame(2, 0, fb); ack != protocol.AckSuccess {
		t.Fatalf("dup ack=%x", ack)
	}
	if writes.Load() != 1 {
		t.Fatalf("dup must not rewrite: writes=%d", writes.Load())
	}
}

// TestSenderWALResetReimport R15 回归：发送端 WAL 重建（seq 从 1 重新编号）重导的
// 帧不得被 "seq<=last_seq" 吞掉——修复前静默丢数据（实测 writes=0）。内容去重窗口
// 只吞"同 seq 且同 CRC"的真重复帧；不同内容必须重新落库（幂等覆盖）。
func TestSenderWALResetReimport(t *testing.T) {
	srv, writes, _ := fakeTarget(t, false)
	seqFile := filepath.Join(t.TempDir(), "last_seq")
	if err := saveLastSeq(seqFile, 250000); err != nil { // 接收端曾有 25 万帧历史
		t.Fatal(err)
	}
	r := newTestReceiver(t, srv, Config{LastSeqFile: seqFile})
	// 发送端 WAL 被重建：全量重导从 seq=1 开始（新内容 ts=1..3）
	for seq := uint64(1); seq <= 3; seq++ {
		fb, _ := protocol.EncodeDataZstd(seq, []byte(fmt.Sprintf(`{"metric":{"__name__":"re"},"values":[1],"timestamps":[%d]}`, seq)+"\n"))
		if ack := r.HandleFrame(7, uint64(seq-1), fb); ack != protocol.AckSuccess {
			t.Fatalf("seq=%d ack=%x, want 0xff", seq, ack)
		}
	}
	if got := writes.Load(); got != 3 {
		t.Fatalf("re-exported frames must be imported: writes=%d, want 3", got)
	}
	// 同内容重发（ACK 丢失重发）：去重窗口命中，不再写
	fb1, _ := protocol.EncodeDataZstd(1, []byte(`{"metric":{"__name__":"re"},"values":[1],"timestamps":[1]}`+"\n"))
	if ack := r.HandleFrame(7, 0, fb1); ack != protocol.AckSuccess {
		t.Fatalf("dup ack=%x", ack)
	}
	if got := writes.Load(); got != 3 {
		t.Fatalf("identical resend must be deduped: writes=%d, want 3", got)
	}
}

// TestDedupWindowZeroCRCMiss R22 回归：未登记 seq 不得与 CRC=0 误命中
// （map 零值陷阱）——否则被 seqJumpLimit 大跳跃越过的区间帧若 CRC 恰为 0，
// 会被当作已处理吞掉造成丢数据。
func TestDedupWindowZeroCRCMiss(t *testing.T) {
	r := &Receiver{dedupCRC: make(map[uint64]uint32)}
	if r.dedupHit(100, 0) {
		t.Fatal("unrecorded seq must not hit even with crc=0")
	}
	if r.dedupHit(100, 1) {
		t.Fatal("unrecorded seq must not hit with any crc")
	}
	r.dedupRecord(100, 0)
	if !r.dedupHit(100, 0) {
		t.Fatal("recorded (seq=100,crc=0) must hit")
	}
	r.dedupRecord(100, 42) // 同 seq 内容更新（重导）：只命中新 CRC
	if r.dedupHit(100, 0) {
		t.Fatal("stale crc must not hit after update")
	}
	if !r.dedupHit(100, 42) {
		t.Fatal("updated crc must hit")
	}
}

// TestSeqJumpedOverFramesReimport R15 附带：大跳跃（>seqJumpLimit）越过的区间帧
// 之后到达（修复前被 seq<=last_seq 吞掉）也必须重新落库。
func TestSeqJumpedOverFramesReimport(t *testing.T) {
	srv, writes, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	// 大跳跃帧先到：last_seq 越过到 200002
	fbJump, _ := protocol.EncodeDataZstd(200002, []byte(`{"metric":{"__name__":"j"},"values":[1],"timestamps":[2]}`+"\n"))
	if ack := r.HandleFrame(1, 0, fbJump); ack != protocol.AckSuccess {
		t.Fatalf("jump ack=%x", ack)
	}
	// 被越过的区间帧到达：不得吞掉
	fb, _ := protocol.EncodeDataZstd(100001, []byte(`{"metric":{"__name__":"j"},"values":[1],"timestamps":[1]}`+"\n"))
	if ack := r.HandleFrame(1, 1, fb); ack != protocol.AckSuccess {
		t.Fatalf("gap frame ack=%x", ack)
	}
	if got := writes.Load(); got != 2 {
		t.Fatalf("gap frame must be imported: writes=%d, want 2", got)
	}
}
