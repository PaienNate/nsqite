package nsqite

import (
	"database/sql"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

func resetWalSingletons() {
	db = nil
	transactionMQ = nil
	once = sync.Once{}
	walPath = ""
}

func mustOpenSQLite(t *testing.T, dbFile string) *DB {
	t.Helper()
	g, err := sql.Open("sqlite", "file:"+dbFile+"?_pragma=synchronous(NORMAL)&_pragma=busy_timeout(15000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	g.SetMaxOpenConns(4)
	d := SetSQLite(g)
	if err := d.AutoMigrate(); err != nil {
		t.Fatal(err)
	}
	return d
}

// TestWALFoldIdempotent 验证折入的正确性和幂等性
func TestWALFoldIdempotent(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "nsqite.wal")
	w, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	db := mustOpenSQLite(t, filepath.Join(t.TempDir(), "nsqite.db"))

	// 写入 P + R
	if _, err := w.AppendPublish(PublishOp{ID: 1, Topic: "t", Body: []byte("hi"), Timestamp: time.Now().UnixMilli(), Consumers: 1, Channels: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendRespond(RespondOp{ID: 1, Channel: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendPublish(PublishOp{ID: 2, Topic: "t", Body: []byte("yo"), Timestamp: time.Now().UnixMilli(), Consumers: 2, Channels: "a,b"}); err != nil {
		t.Fatal(err)
	}

	ops, next, err := w.ReadOps(w.Watermark())
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 3 {
		t.Fatalf("expected 3 ops, got %d", len(ops))
	}
	if err := db.Fold(ops); err != nil {
		t.Fatal(err)
	}
	if err := w.SetWatermark(next); err != nil {
		t.Fatal(err)
	}

	count, err := db.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}

	var responded uint32
	if err := db.DB.QueryRow(`SELECT responded FROM nsqite_messages WHERE id = 1`).Scan(&responded); err != nil {
		t.Fatal(err)
	}
	if responded != 1 {
		t.Fatalf("expected responded=1 for id=1, got %d", responded)
	}

	// 幂等性：再次应用同一批 op 不应重复计数
	if err := db.Fold(ops); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.QueryRow(`SELECT responded FROM nsqite_messages WHERE id = 1`).Scan(&responded); err != nil {
		t.Fatal(err)
	}
	if responded != 1 {
		t.Fatalf("idempotency violated: responded=%d", responded)
	}
}

// TestWALPublishFlow 端到端：Publish → 内存投递 → Finish(R-op) → 折入 → 重启重放
func TestWALPublishFlow(t *testing.T) {
	resetWalSingletons()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "nsqite.db")
	walPath = filepath.Join(dir, "nsqite.wal")

	mustOpenSQLite(t, dbFile)

	const topic = "wal-topic"
	const n = 200
	p := NewProducer()
	c := NewConsumer(topic, "ch")
	c.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error {
		return nil
	}), 4)

	for i := 0; i < n; i++ {
		if err := p.Publish(topic, []byte("msg")); err != nil {
			t.Fatal(err)
		}
	}
	c.WaitMessage()
	time.Sleep(50 * time.Millisecond)

	// 强制折入，确认 SQLite 已见全量 + responded 正确
	TransactionMQ().foldAll()
	count, err := TransactionMQ().db.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("expected %d rows after fold, got %d", n, count)
	}
	var responded, consumers uint32
	if err := TransactionMQ().db.DB.QueryRow(`SELECT responded, consumers FROM nsqite_messages WHERE id = 1`).Scan(&responded, &consumers); err != nil {
		t.Fatal(err)
	}
	if responded != consumers || responded == 0 {
		t.Fatalf("expected responded==consumers>0, got responded=%d consumers=%d", responded, consumers)
	}
}

// TestWALRestart 模拟崩溃重启：WAL 里有未折入的 op，新实例应折入并重放待处理消息
func TestWALRestart(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "nsqite.db")
	walFile := filepath.Join(dir, "nsqite.wal")

	// 第一个“会话”：直接向 WAL 写两条发布 op，故意不折入（模拟崩溃前 ack 但未落库）
	w, err := OpenWAL(walFile)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.AppendPublish(PublishOp{ID: 1, Topic: "restart-topic", Body: []byte("a"), Timestamp: time.Now().UnixMilli(), Consumers: 1, Channels: "r"})
	_, _ = w.AppendPublish(PublishOp{ID: 2, Topic: "restart-topic", Body: []byte("b"), Timestamp: time.Now().UnixMilli(), Consumers: 1, Channels: "r"})
	_ = w.Close()

	// 重启：重建单例 + DB，新 NSQite 自动折入并触发重放
	resetWalSingletons()
	walPath = walFile
	d := mustOpenSQLite(t, dbFile)
	n := newNSQite(d)

	var received int32
	got := make(chan struct{})
	c := NewConsumer("restart-topic", "r")
	c.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error {
		if atomic.AddInt32(&received, 1) == 2 {
			close(got)
		}
		return nil
	}), 2)

	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatalf("restart did not redeliver pending messages, received=%d", atomic.LoadInt32(&received))
	}

	if count, _ := n.db.Count(); count != 2 {
		t.Fatalf("expected 2 rows persisted after restart, got %d", count)
	}
}
