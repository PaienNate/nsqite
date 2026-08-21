package nsqite

import (
	"database/sql"
	"strconv"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

// bench 数据：总数 pendingTotal，其中每 interval 行产生一条待处理记录（稀疏分布在大表各处）
const (
	pendingTotal  = 200000
	pendingSparse = 2000 // 待处理总条数
	pendingInterval = pendingTotal / pendingSparse
)

func newPendingBenchDB(b *testing.B) *sql.DB {
	b.Helper()
	file := "/tmp/nsqite_pending_bench.db"
	removeSQLiteFiles(file)
	b.Cleanup(func() { removeSQLiteFiles(file) })

	g, err := sql.Open("sqlite", "file:"+file+"?_pragma=synchronous=NORMAL&_pragma=journal_mode(WAL)")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	g.SetMaxOpenConns(1)
	if err := SetSQLite(g).AutoMigrate(); err != nil {
		b.Fatal(err)
	}

	tx, err := g.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO nsqite_messages (topic, body, consumers, responded, responded_channels, attempts, pending)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < pendingTotal; i++ {
		consumers := 2
		responded := 2 // 已完成
		pending := 0
		if i%pendingInterval == 0 {
			responded = 0
			pending = 1
		}
		if _, err := stmt.Exec("t", []byte("body-"+strconv.Itoa(i)), consumers, responded, "a,b", 0, pending); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return g
}

// drainPendingBench 按 messagePump 方式反复 LIMIT 100 并推进游标，直至排空待处理
func drainPendingBench(b *testing.B, where string) {
	g := newPendingBenchDB(b)
	query := "SELECT id FROM nsqite_messages WHERE id > ? AND " + where + " ORDER BY id ASC LIMIT 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := 0
		for {
			rows, err := g.Query(query, id)
			if err != nil {
				b.Fatal(err)
			}
			n, last := 0, 0
			var x int
			for rows.Next() {
				_ = rows.Scan(&x)
				last = x
				n++
			}
			_ = rows.Close()
			if n == 0 {
				break
			}
			id = last
		}
	}
	b.StopTimer()
}

// 新查询：pending 派生列 + (pending, id) 索引
func BenchmarkFetchPendingNew(b *testing.B) {
	drainPendingBench(b, "pending = 1")
}

// 旧查询：responded < consumers（仅 (consumers, responded) 索引）
func BenchmarkFetchPendingOld(b *testing.B) {
	drainPendingBench(b, "responded < consumers")
}
