// backfillInit 回归测试（移植自 fork app/vm-sync vmsync_test.go，R19/R23）。
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"vm-sync/internal/config"
	"vm-sync/internal/vm"
	"vm-sync/internal/wal"
)

func newTestVMClient(t *testing.T, url string) *vm.Client {
	t.Helper()
	c, err := vm.NewClient(vm.Config{URL: url, Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func openTestWAL(t *testing.T) *wal.WAL {
	t.Helper()
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

// probeOldestHandler 构造"库内最早数据=oldestMs"的假 export 端点：
// end 超过 oldestMs 时返回一行数据，否则空响应——ProbeOldestData 二分收敛到 oldestMs。
func probeOldestHandler(oldestMs func() int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		endStr := r.URL.Query().Get("end")
		endSec, err := strconv.ParseFloat(endStr, 64)
		if err == nil && int64(endSec*1000) > oldestMs() {
			fmt.Fprint(w, `{"metric":{"__name__":"m"},"values":[1],"timestamps":[1]}`+"\n")
			return
		}
		w.WriteHeader(http.StatusOK) // 空
	}
}

// TestBackfillInitProbeFailureNotRecorded R19 回归：backfill=all 且最老数据
// 探测失败时，本次启动不得记录/应用回填策略（修复前 policy=-1/boundary=0 落盘，
// 策略从此不再变化，backfill=all 静默退化为永久实时模式，历史数据永久漏发）。
func TestBackfillInitProbeFailureNotRecorded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	vc := newTestVMClient(t, srv.URL)
	w := openTestWAL(t)
	now := time.Now().UnixMilli()
	watermarkMs := int64(1000)
	if err := backfillInit(zap.NewNop(), vc, w,
		config.BackfillSpec{Mode: config.BackfillAll}, watermarkMs, now); err != nil {
		t.Fatalf("backfillInit must degrade gracefully on probe failure: %v", err)
	}
	if got := w.BackfillPolicyNs(); got != wal.BackfillNoneNs {
		t.Fatalf("policy must NOT be recorded on probe failure, got %d", got)
	}
	if c := w.Cursor(); c != now-watermarkMs {
		t.Fatalf("cursor=%d want %d (now-watermark)", c, now-watermarkMs)
	}
}

// TestBackfillInitProbeSuccessEmptyDoesNotRecordPolicy R23 回归：探测成功但库为空
// 时不得记录策略（修复前 policy=-1/boundary=0 落盘——游标永久锁在实时起点，
// 后续导入的历史数据重启后也不会回拨，永久漏发）。
func TestBackfillInitProbeSuccessEmptyDoesNotRecordPolicy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // 空响应 = 空库
	}))
	t.Cleanup(srv.Close)
	vc := newTestVMClient(t, srv.URL)
	w := openTestWAL(t)
	now := time.Now().UnixMilli()
	if err := backfillInit(zap.NewNop(), vc, w,
		config.BackfillSpec{Mode: config.BackfillAll}, 1000, now); err != nil {
		t.Fatal(err)
	}
	if got := w.BackfillPolicyNs(); got != wal.BackfillNoneNs {
		t.Fatalf("policy must NOT be recorded on empty db, got %d", got)
	}
	if got, want := w.Cursor(), now-1000; got != want {
		t.Fatalf("cursor=%d want %d (now-watermark)", got, want)
	}
}

// TestBackfillInitEmptyThenDataRewinds R23 回归（全场景）：空库首启不记录策略，
// 数据出现后重启探测成功 → 策略回拨到库内最早数据（修复前策略已被锁死，
// 重启不回拨，历史数据永久漏发）。
func TestBackfillInitEmptyThenDataRewinds(t *testing.T) {
	var oldest atomic.Int64 // 0=空库
	srv := httptest.NewServer(probeOldestHandler(oldest.Load))
	t.Cleanup(srv.Close)
	vc := newTestVMClient(t, srv.URL)
	w := openTestWAL(t)
	now := time.Now().UnixMilli()

	// 第一次启动：空库 → 不记录策略、游标从水位起
	if err := backfillInit(zap.NewNop(), vc, w,
		config.BackfillSpec{Mode: config.BackfillAll}, 1000, now); err != nil {
		t.Fatal(err)
	}
	if got := w.BackfillPolicyNs(); got != wal.BackfillNoneNs {
		t.Fatalf("empty db boot: policy=%d want 0", got)
	}
	if got, want := w.Cursor(), now-1000; got != want {
		t.Fatalf("empty db boot: cursor=%d want %d", got, want)
	}

	// 数据出现后重启：探测到最早数据 → 记录策略并回拨游标
	oldest.Store(now - int64(time.Hour/time.Millisecond))
	if err := backfillInit(zap.NewNop(), vc, w,
		config.BackfillSpec{Mode: config.BackfillAll}, 1000, now+60_000); err != nil {
		t.Fatal(err)
	}
	if got := w.BackfillPolicyNs(); got != wal.BackfillAllNs {
		t.Fatalf("restart with data: policy=%d want %d", got, wal.BackfillAllNs)
	}
	if got, want := w.Cursor(), oldest.Load(); got != want {
		t.Fatalf("restart with data: cursor=%d want %d (rewound to oldest)", got, want)
	}

	// 再次重启（策略已记录）：不回拨、不丢游标
	c := w.Cursor()
	if err := backfillInit(zap.NewNop(), vc, w,
		config.BackfillSpec{Mode: config.BackfillAll}, 1000, now+120_000); err != nil {
		t.Fatal(err)
	}
	if got := w.Cursor(); got != c {
		t.Fatalf("policy-unchanged restart must not rewind: cursor=%d want %d", got, c)
	}
}

// TestBackfillInitEmptyDurationDoesNotRecordThenRewinds R23 回归（有界回填）：
// 空库首启不记录策略；数据出现后重启按 Nd 窗口回拨。
func TestBackfillInitEmptyDurationDoesNotRecordThenRewinds(t *testing.T) {
	var oldest atomic.Int64
	srv := httptest.NewServer(probeOldestHandler(oldest.Load))
	t.Cleanup(srv.Close)
	vc := newTestVMClient(t, srv.URL)
	w := openTestWAL(t)
	now := time.Now().UnixMilli()
	const days = 30
	spec := config.BackfillSpec{Mode: config.BackfillDuration, Dur: time.Duration(days) * 24 * time.Hour}

	// 空库首启：不记录
	if err := backfillInit(zap.NewNop(), vc, w, spec, 1000, now); err != nil {
		t.Fatal(err)
	}
	if got := w.BackfillPolicyNs(); got != wal.BackfillNoneNs {
		t.Fatalf("empty db boot: policy=%d want 0", got)
	}

	// 数据出现后重启：记录 Nd 并回拨到最早数据（1h 前，在 Nd 窗口内）
	oldest.Store(now - int64(time.Hour/time.Millisecond))
	if err := backfillInit(zap.NewNop(), vc, w, spec, 1000, now+60_000); err != nil {
		t.Fatal(err)
	}
	policyNs := int64(days * 24 * 3600 * 1000)
	if got := w.BackfillPolicyNs(); got != policyNs {
		t.Fatalf("restart with data: policy=%d want %d", got, policyNs)
	}
	if got, want := w.Cursor(), oldest.Load(); got != want {
		t.Fatalf("restart with data: cursor=%d want %d (rewound to oldest)", got, want)
	}
}

// TestBackfillInitDurationProbeFailureStillApplies R19：有界回填（Nd）不依赖
// 探测——探测失败时边界=now-Nd 必然有效，策略必须照常应用并初始化游标。
func TestBackfillInitDurationProbeFailureStillApplies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	vc := newTestVMClient(t, srv.URL)
	w := openTestWAL(t)
	now := time.Now().UnixMilli()
	const days = 30
	policyNs := int64(days * 24 * 3600 * 1000)
	if err := backfillInit(zap.NewNop(), vc, w,
		config.BackfillSpec{Mode: config.BackfillDuration, Dur: time.Duration(days) * 24 * time.Hour},
		1000, now); err != nil {
		t.Fatalf("duration backfill must apply despite probe failure: %v", err)
	}
	if got := w.BackfillPolicyNs(); got != policyNs {
		t.Fatalf("policy=%d want %d", got, policyNs)
	}
	if got, want := w.Cursor(), now-policyNs; got != want {
		t.Fatalf("cursor=%d want %d (now-Nd)", got, want)
	}
}

// TestBackfillInitRealtimeOnly R19 对照：backfill=0 不探测、策略记录 0、
// 游标从水位起。
func TestBackfillInitRealtimeOnly(t *testing.T) {
	vc := newTestVMClient(t, "http://127.0.0.1:1") // 无需探测，地址不可达也不影响
	w := openTestWAL(t)
	now := time.Now().UnixMilli()
	if err := backfillInit(zap.NewNop(), vc, w,
		config.BackfillSpec{Mode: config.BackfillNone}, 1000, now); err != nil {
		t.Fatal(err)
	}
	if got := w.BackfillPolicyNs(); got != wal.BackfillNoneNs {
		t.Fatalf("policy=%d want 0", got)
	}
	if got, want := w.Cursor(), now-1000; got != want {
		t.Fatalf("cursor=%d want %d (now-watermark)", got, want)
	}
}
