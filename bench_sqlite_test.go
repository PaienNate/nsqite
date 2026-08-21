package nsqite

import (
	"database/sql"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

// removeSQLiteFiles 删除 db 及其伴随文件，避免上次残留的热日志导致只读/表缺失
func removeSQLiteFiles(dbFile string) {
	for _, f := range []string{dbFile, dbFile + "-journal", dbFile + "-wal", dbFile + "-shm"} {
		_ = os.Remove(f)
	}
}

// resetSingletonsForBench 重置包级单例，保证 Go 基准的多轮运行互不污染
func resetSingletonsForBench() {
	db = nil
	transactionMQ = nil
	once = sync.Once{}
}

// benchSQLitePublish 走完整链路（Publish 写库 + 投递 + Finish 回写）测量吞吐
func benchSQLitePublish(b *testing.B, dbFile, dsn string) {
	resetSingletonsForBench()
	removeSQLiteFiles(dbFile)
	defer removeSQLiteFiles(dbFile)

	g, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	g.SetMaxOpenConns(runtime.NumCPU())
	g.SetMaxIdleConns(runtime.NumCPU())

	if err := SetSQLite(g).AutoMigrate(); err != nil {
		b.Fatal(err)
	}
	slog.SetLogLoggerLevel(slog.LevelError)

	const topic = "bench-topic"
	p := NewProducer()
	c := NewConsumer(topic, "channel")
	c2 := NewConsumer(topic, "channel2")

	c.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error { return nil }), int32(runtime.NumCPU()))
	c2.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error { return nil }), int32(runtime.NumCPU()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Publish(topic, []byte("test message")); err != nil {
			b.Fatalf("publish %d: %v", i, err)
		}
	}
	b.StopTimer()
	c.WaitMessage()
	c2.WaitMessage()
	time.Sleep(100 * time.Millisecond)
	c.Stop()
	c2.Stop()
	TransactionMQ().Close()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

func BenchmarkSQLitePublish(b *testing.B) {
	benchSQLitePublish(b, "/tmp/nsqite_bench_publish.db", "/tmp/nsqite_bench_publish.db")
}

func BenchmarkSQLitePublishOpt(b *testing.B) {
	benchSQLitePublish(b, "/tmp/nsqite_bench_publish_opt.db",
		"file:/tmp/nsqite_bench_publish_opt.db?_pragma=synchronous(NORMAL)&_pragma=busy_timeout(15000)&_pragma=cache_size(-200000)")
}

// benchSQLitePublishSplit 走读写连接池拆分（写池 1 连接 + 读池 N 连接）
func benchSQLitePublishSplit(b *testing.B, dbFile, dsn string) {
	resetSingletonsForBench()
	removeSQLiteFiles(dbFile)
	defer removeSQLiteFiles(dbFile)

	w, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatal(err)
	}
	r, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	defer r.Close()

	d := SetSQLiteWithPool(w, r)
	if err := d.AutoMigrate(); err != nil {
		b.Fatal(err)
	}
	slog.SetLogLoggerLevel(slog.LevelError)

	const topic = "bench-topic"
	p := NewProducer()
	c := NewConsumer(topic, "channel")
	c2 := NewConsumer(topic, "channel2")
	c.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error { return nil }), int32(runtime.NumCPU()))
	c2.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error { return nil }), int32(runtime.NumCPU()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Publish(topic, []byte("test message")); err != nil {
			b.Fatalf("publish %d: %v", i, err)
		}
	}
	b.StopTimer()
	c.WaitMessage()
	c2.WaitMessage()
	time.Sleep(100 * time.Millisecond)
	c.Stop()
	c2.Stop()
	TransactionMQ().Close()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

func BenchmarkSQLitePublishSplitPools(b *testing.B) {
	benchSQLitePublishSplit(b, "/tmp/nsqite_bench_publish_split.db",
		"file:/tmp/nsqite_bench_publish_split.db?_pragma=synchronous(NORMAL)&_pragma=busy_timeout(15000)&_pragma=cache_size(-200000)")
}

func removeWalFiles(walFile string) {
	_ = os.Remove(walFile)
	_ = os.Remove(walFile + ".wm")
}

// benchSQLitePublishWAL 走 WAL-first：Publish/Finish 只写 AppWAL（每条 fsync），SQLite 由 flusher 批量折入
func benchSQLitePublishWAL(b *testing.B, dbFile, walFile, dsn string) {
	benchSQLitePublishWALCfg(b, dbFile, walFile, dsn, 2, 0)
}

func benchSQLitePublishWALCfg(b *testing.B, dbFile, walFile, dsn string, level int, period time.Duration) {
	resetSingletonsForBench()
	walPath = walFile
	SetWALLevel(level)
	SetWALFsyncPeriod(period)
	removeSQLiteFiles(dbFile)
	removeWalFiles(walFile)
	defer removeSQLiteFiles(dbFile)
	defer removeWalFiles(walFile)
	defer func() { walPath = ""; SetWALLevel(2); SetWALFsyncPeriod(0) }()

	w, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatal(err)
	}
	r, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	defer r.Close()

	d := SetSQLiteWithPool(w, r)
	if err := d.AutoMigrate(); err != nil {
		b.Fatal(err)
	}
	slog.SetLogLoggerLevel(slog.LevelError)

	const topic = "bench-topic"
	p := NewProducer()
	c := NewConsumer(topic, "channel")
	c2 := NewConsumer(topic, "channel2")
	c.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error { return nil }), int32(runtime.NumCPU()))
	c2.AddConcurrentHandlers(ConsumerHandlerFunc(func(*Message) error { return nil }), int32(runtime.NumCPU()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Publish(topic, []byte("test message")); err != nil {
			b.Fatalf("publish %d: %v", i, err)
		}
	}
	b.StopTimer()
	c.WaitMessage()
	c2.WaitMessage()
	time.Sleep(100 * time.Millisecond)
	c.Stop()
	c2.Stop()
	TransactionMQ().Close()
	TransactionMQ().foldAll()
	time.Sleep(100 * time.Millisecond)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

func BenchmarkSQLitePublishWAL(b *testing.B) {
	benchSQLitePublishWAL(b, "/tmp/nsqite_bench_wal.db", "/tmp/nsqite_bench_wal.wal",
		"file:/tmp/nsqite_bench_wal.db?_pragma=synchronous(NORMAL)&_pragma=busy_timeout(15000)&_pragma=cache_size(-200000)")
}

func BenchmarkSQLitePublishWALGroup(b *testing.B) {
	benchSQLitePublishWALCfg(b, "/tmp/nsqite_bench_walg.db", "/tmp/nsqite_bench_walg.wal",
		"file:/tmp/nsqite_bench_walg.db?_pragma=synchronous(NORMAL)&_pragma=busy_timeout(15000)&_pragma=cache_size(-200000)", 2, 5*time.Millisecond)
}

func BenchmarkSQLitePublishWALNoSync(b *testing.B) {
	benchSQLitePublishWALCfg(b, "/tmp/nsqite_bench_waln.db", "/tmp/nsqite_bench_waln.wal",
		"file:/tmp/nsqite_bench_waln.db?_pragma=synchronous(NORMAL)&_pragma=busy_timeout(15000)&_pragma=cache_size(-200000)", 1, 0)
}

// benchSQLiteCreate 只测 Publish 的 INSERT 落库路径，隔离出文章最关注的写入成本
func benchSQLiteCreate(b *testing.B, dbFile string) {
	resetSingletonsForBench()
	removeSQLiteFiles(dbFile)
	defer removeSQLiteFiles(dbFile)

	g, err := sql.Open("sqlite", dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	g.SetMaxOpenConns(runtime.NumCPU())
	g.SetMaxIdleConns(runtime.NumCPU())

	d := SetSQLite(g)
	if err := d.AutoMigrate(); err != nil {
		b.Fatal(err)
	}
	slog.SetLogLoggerLevel(slog.LevelError)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.Create(&Message{Topic: "bench-topic", Body: []byte("test message"), Timestamp: time.Now()}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

func BenchmarkSQLiteCreate(b *testing.B) {
	benchSQLiteCreate(b, "/tmp/nsqite_bench_create.db")
}
