package nsqite

import (
	"path/filepath"
	"testing"
	"time"
)

// TestSMAStatsCountAndPending 验证直接（非 WAL）模式下 pending 列与 stats 计数的一致性
func TestSMAStatsCountAndPending(t *testing.T) {
	resetWalSingletons()
	db := mustOpenSQLite(t, filepath.Join(t.TempDir(), "nsqite.db"))

	const topic = "sma-topic"
	const n = 50
	p := NewProducer()
	c := NewConsumer(topic, "ch")
	c.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error { return nil }), 8)
	for i := 0; i < n; i++ {
		if err := p.Publish(topic, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	c.WaitMessage()
	time.Sleep(100 * time.Millisecond)

	count, err := db.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("stats count = %d, want %d", count, n)
	}

	var pending, responded, consumers uint32
	if err := db.DB.QueryRow(`SELECT pending, responded, consumers FROM nsqite_messages ORDER BY id LIMIT 1`).
		Scan(&pending, &responded, &consumers); err != nil {
		t.Fatal(err)
	}
	if pending != 0 || responded != consumers {
		t.Fatalf("expected completed (pending=0 responded==consumers), got pending=%d responded=%d consumers=%d", pending, responded, consumers)
	}
}

// TestWALFsyncPeriodConfig 验证组提交（period fsync）模式下 WAL 仍可正确写读
func TestWALFsyncPeriodConfig(t *testing.T) {
	dir := t.TempDir()
	walFile := filepath.Join(dir, "nsqite.wal")

	SetWALLevel(2)
	SetWALFsyncPeriod(2 * time.Millisecond)
	defer func() { SetWALLevel(2); SetWALFsyncPeriod(0) }()

	w, err := OpenWAL(walFile)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if _, err := w.AppendPublish(PublishOp{ID: i, Topic: "t", Body: []byte("b"), Timestamp: time.Now().UnixMilli(), Consumers: 1, Channels: "c"}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond) // 让组提交 fsync 跑起来
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(walFile)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	ops, _, err := w2.ReadOps(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 3 {
		t.Fatalf("group-commit WAL read %d ops, want 3", len(ops))
	}
}
