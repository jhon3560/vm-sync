package sender

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"vm-sync/internal/monitor"
	"vm-sync/internal/protocol"
	"vm-sync/internal/transport"
	"vm-sync/internal/wal"
)

// ackServer 可控 ACK 行为的假 Receiver（移植自 influx-sync sender 测试，本轮新增
// sender 单测覆盖：停等/重试不丢/退避/滑窗——此前 sender 完全无单测，pipeline
// 路径零覆盖）。alwaysNack=恒 0x00；否则仅对 seq==nackOnceSeq 的帧 nack 一次
// （按帧头 seq 判定——server 对帧的处理顺序不保证与 seq 一致，按收到次序判定
// 会有竞态），其余 0xff。
func ackServer(t *testing.T, alwaysNack bool, nackOnceSeq uint64) (addr string, received *atomic.Int64) {
	t.Helper()
	received = &atomic.Int64{}
	var nacked atomic.Bool
	srv := transport.NewServer(transport.ServerConfig{Listen: "127.0.0.1:0"}, func(id uint64, _ uint64, fb []byte) byte {
		received.Add(1)
		if alwaysNack {
			return protocol.AckFail
		}
		if h, err := protocol.ParseHeader(fb); err == nil && h.Seq == nackOnceSeq && nacked.CompareAndSwap(false, true) {
			return protocol.AckFail
		}
		return protocol.AckSuccess
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx)
	t.Cleanup(func() { cancel(); srv.Close() })
	return srv.Addr().String(), received
}

// newSenderEnv 建 WAL + metrics + 两帧数据。
func newSenderEnv(t *testing.T) (*wal.WAL, *monitor.Metrics) {
	t.Helper()
	w, err := wal.Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	m := monitor.New()
	if _, err := w.Append(protocol.TypeData, []byte(`{"metric":{"__name__":"m"},"values":[1],"timestamps":[1]}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(protocol.TypeData, []byte(`{"metric":{"__name__":"m"},"values":[2],"timestamps":[2]}`)); err != nil {
		t.Fatal(err)
	}
	return w, m
}

// TestSenderNormalAck 停等正常路径：两帧全部 ACK → WAL 排空。
func TestSenderNormalAck(t *testing.T) {
	addr, received := ackServer(t, false, 0)
	w, m := newSenderEnv(t)
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, zap.NewNop(), SenderConfig{IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for i := 0; i < 100; i++ {
		if w.PendingCount() == 0 && received.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending=%d received=%d", w.PendingCount(), received.Load())
	}
}

// TestSenderNackNeverDrops At-Least-Once 红线：持续 0x00 重试超限后帧必须留在
// WAL，绝不丢弃（毒丸由 receiver 侧 DLQ 隔离）。
func TestSenderNackNeverDrops(t *testing.T) {
	addr, _ := ackServer(t, true, 0) // 恒 nack
	w, m := newSenderEnv(t)
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, zap.NewNop(), SenderConfig{
		MaxRetry: 3, BackoffBase: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond,
		IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	<-done
	if w.PendingCount() != 2 {
		t.Fatalf("frames must stay in wal (at-least-once), pending=%d", w.PendingCount())
	}
	if m.DLQCount() != 0 {
		t.Fatalf("dlq must be 0, got %d", m.DLQCount())
	}
}

// TestSenderRetryRecovers nack 一次后 ack → 重试成功、WAL 排空。
func TestSenderRetryRecovers(t *testing.T) {
	addr, received := ackServer(t, false, 1) // seq=1 帧 nack 一次 → 重试成功
	w, m := newSenderEnv(t)
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, zap.NewNop(), SenderConfig{
		MaxRetry: 5, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for i := 0; i < 200; i++ {
		if w.PendingCount() == 0 && received.Load() >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending=%d received=%d", w.PendingCount(), received.Load())
	}
	if received.Load() < 3 {
		t.Fatalf("expected >=3 sends (nack + retries), got %d", received.Load())
	}
}

// TestSenderDisconnectKeepsWAL 服务器不存在：连接失败，WAL 数据保留。
func TestSenderDisconnectKeepsWAL(t *testing.T) {
	w, m := newSenderEnv(t)
	client := transport.NewClient(transport.ClientConfig{Addr: "127.0.0.1:1", Timeout: 200 * time.Millisecond})
	s := NewSender(w, client, m, zap.NewNop(), SenderConfig{
		MaxRetry: 2, BackoffBase: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond,
		IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	<-done
	if w.PendingCount() != 2 {
		t.Fatalf("frames must stay in wal on disconnect, pending=%d", w.PendingCount())
	}
}

// TestSenderPipelineAcksInOrder 滑窗（A1）：pipeline=2 两帧在途按序 ACK 全确认。
func TestSenderPipelineAcksInOrder(t *testing.T) {
	addr, received := ackServer(t, false, 0)
	w, m := newSenderEnv(t)
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, zap.NewNop(), SenderConfig{
		Pipeline: 2, IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for i := 0; i < 100; i++ {
		if w.PendingCount() == 0 && received.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pipeline: pending=%d received=%d", w.PendingCount(), received.Load())
	}
}

// TestSenderPipelineGoBackN 滑窗第 k 帧 0x00 → 连接级失败重连 + go-back-N 重发
// 尾窗（N1 语义：关连接排干陈旧 ACK，杜绝 ACK 错位提交未写库帧）。
func TestSenderPipelineGoBackN(t *testing.T) {
	addr, received := ackServer(t, false, 2) // seq=2 帧 nack 一次 → go-back-N 重发尾窗
	w, m := newSenderEnv(t)
	w3, err := w.Append(protocol.TypeData, []byte(`{"metric":{"__name__":"m"},"values":[3],"timestamps":[3]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = w3
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, zap.NewNop(), SenderConfig{
		Pipeline: 3, MaxRetry: 10, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for i := 0; i < 400; i++ {
		if w.PendingCount() == 0 && received.Load() >= 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("go-back-N must converge: pending=%d received=%d", w.PendingCount(), received.Load())
	}
	// 3 帧首轮 + 第 2 帧 0x00 触发 go-back-N 重发尾窗（第 2/3 帧）= 5 次发送
	if received.Load() < 5 {
		t.Fatalf("expected >=5 sends (3 + 尾窗重发 2), got %d", received.Load())
	}
	if m.DLQCount() != 0 {
		t.Fatalf("dlq must be 0, got %d", m.DLQCount())
	}
}

// TestBackoff 退避序列：1,2,4,...封顶。
func TestBackoff(t *testing.T) {
	s := &Sender{cfg: SenderConfig{BackoffBase: time.Second, BackoffMax: 60 * time.Second}}
	cases := []struct {
		n    int
		want time.Duration
	}{{0, 0}, {1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {10, 60 * time.Second}}
	for _, c := range cases {
		if got := s.backoff(c.n); got != c.want {
			t.Fatalf("backoff(%d)=%v want %v", c.n, got, c.want)
		}
	}
}
