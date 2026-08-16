package vm

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := NewClient(Config{URL: srv.URL, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestExportRangeAndImportRoundtrip(t *testing.T) {
	var imported []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/export":
			// 返回 2 行样本（与 import 格式对称）
			fmt.Fprint(w, `{"metric":{"__name__":"cpu","job":"a"},"values":[1.5,2.5],"timestamps":[1786800000000,1786800001000]}`+"\n"+
				`{"metric":{"__name__":"mem","job":"b"},"values":[3],"timestamps":[1786800002000]}`+"\n")
		case "/api/v1/import":
			buf := make([]byte, 1<<20)
			n, _ := r.Body.Read(buf)
			imported = append(imported, buf[:n]...)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := newTestClient(t, srv)

	raw, err := c.ExportRange(context.Background(), 1786800000000, 1786800003000)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"__name__":"cpu"`) || !strings.Contains(string(raw), `"__name__":"mem"`) {
		t.Fatalf("export body: %s", raw)
	}
	if err := c.ImportWrite(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if string(imported) != string(raw) {
		t.Fatalf("import must be verbatim: got %q want %q", imported, raw)
	}
}

func TestExportHasData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("start") == "1786800000.000000" {
			// 该窗口有数据
			fmt.Fprint(w, `{"metric":{"__name__":"cpu"},"values":[1],"timestamps":[1786800000000]}`+"\n")
			return
		}
		// 其他窗口为空
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	has, err := c.ExportHasData(context.Background(), 1786800000000, 1786800001000)
	if err != nil || !has {
		t.Fatalf("has=%v err=%v", has, err)
	}
	has, err = c.ExportHasData(context.Background(), 1790000000000, 1790000001000)
	if err != nil || has {
		t.Fatalf("empty window: has=%v err=%v", has, err)
	}
}

// fakeVM 可控最早数据的假 VM：数据存在于 [oldest, now)，export 按窗口返回样本行。
type fakeVM struct {
	oldest int64
	now    int64
}

func (f *fakeVM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/v1/export" {
		http.NotFound(w, r)
		return
	}
	start := parseSec(r.URL.Query().Get("start"))
	end := parseSec(r.URL.Query().Get("end"))
	// 窗口 [start,end) 必须真正包含数据点（ts=oldest）才返回
	if f.oldest > 0 && start < f.oldest && end > f.oldest {
		fmt.Fprintf(w, `{"metric":{"__name__":"cpu"},"values":[1],"timestamps":[%d]}`+"\n", f.oldest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func parseSec(s string) int64 {
	var sec float64
	fmt.Sscanf(s, "%f", &sec)
	return int64(sec * 1000)
}

func TestProbeOldestData(t *testing.T) {
	now := time.Now().UnixMilli()
	oldest := now - int64(10*time.Minute/time.Millisecond)
	f := &fakeVM{oldest: oldest, now: now}
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := newTestClient(t, srv)
	got, err := c.ProbeOldestData(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 二分收敛到毫秒级
	if got < oldest-1000 || got > oldest+1000 {
		t.Fatalf("probe=%d want≈%d", got, oldest)
	}
}

func TestProbeOldestDataEmpty(t *testing.T) {
	f := &fakeVM{oldest: 0, now: time.Now().UnixMilli()}
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := newTestClient(t, srv)
	got, err := c.ProbeOldestData(context.Background())
	if err != nil || got != 0 {
		t.Fatalf("empty db: got=%d err=%v, want 0", got, err)
	}
}

func TestImportHTTPErrorTyped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "bad request")
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	err := c.ImportWrite(context.Background(), []byte("x\n"))
	if err == nil {
		t.Fatal("want error")
	}
	he, ok := err.(*ImportHTTPError)
	if !ok || he.StatusCode != 400 {
		t.Fatalf("err=%v", err)
	}
}

func TestLastSampleTimestamp(t *testing.T) {
	raw := []byte(`{"metric":{"__name__":"a"},"values":[1,2],"timestamps":[100,200]}` + "\n" +
		`{"metric":{"__name__":"b"},"values":[3],"timestamps":[1500]}` + "\n" +
		`{"metric":{"__name__":"c"},"values":[4,5,6],"timestamps":[300,400,777]}` + "\n")
	// 语义：返回全部行中的最大（最新）样本时间戳
	if got := LastSampleTimestamp(raw); got != 1500 {
		t.Fatalf("last ts=%d want 1500", got)
	}
	// 单行多样本：取 timestamps 数组最后一个元素
	single := []byte(`{"metric":{"__name__":"x"},"values":[1,2,3],"timestamps":[100,200,3210]}`)
	if got := LastSampleTimestamp(single); got != 3210 {
		t.Fatalf("single-line last ts=%d want 3210", got)
	}
}

// TestExportRespTooLarge N15 回归：export 响应超过上限必须显式报错（可操作提示），
// 不得静默截断成半截 JSON lines 落库丢尾部数据。
func TestExportRespTooLarge(t *testing.T) {
	old := maxExportRespBytes
	maxExportRespBytes = 1024
	defer func() { maxExportRespBytes = old }()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/export" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"metric":{"__name__":"m"},"values":[1],"timestamps":[0]}`+"\n"+strings.Repeat("x", 2048)+"\n")
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	_, err := c.ExportRange(context.Background(), 0, 1000)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected truncation error with guidance, got %v", err)
	}
}
