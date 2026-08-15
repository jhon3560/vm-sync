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
