package nsqite

import (
	"database/sql"
	"fmt"
	"runtime"
	"time"
)

const (
	DriverNameSQLite   = "sqlite"
	DriverNamePostgres = "postgres"
	DriverNameOther    = "other"
)

type DB struct {
	// 内嵌主库（写库）。兼容旧用法 d.DB / d.Exec
	*sql.DB
	driverName string
	readDB     *sql.DB // 可选的独立读库
}

var db *DB

// newDB 构造 DB，主库即写库
func newDB(driverName string, write, read *sql.DB) *DB {
	db = &DB{DB: write, driverName: driverName, readDB: read}
	return db
}

// SetDB 初始化 db（兼容旧用法）。
// SQLite 单库时自动按“单写者”调优：SetMaxOpenConns(1)。
func SetDB(driverName string, g *sql.DB) *DB {
	if driverName == DriverNameSQLite {
		tuneSQLiteWritePool(g)
	}
	return newDB(driverName, g, nil)
}

// SetSQLite 初始化 db（兼容旧用法），单库自动单写者调优
func SetSQLite(g *sql.DB) *DB {
	return SetDB(DriverNameSQLite, g)
}

// SetPostgres 初始化 db（兼容旧用法）
func SetPostgres(g *sql.DB) *DB {
	return newDB(DriverNamePostgres, g, nil)
}

// SetDBWithPool 分别指定读写连接池。
// SQLite 写池自动单写者（SetMaxOpenConns(1)），读池按 CPU 数扩容。
func SetDBWithPool(driverName string, write, read *sql.DB) *DB {
	if driverName == DriverNameSQLite {
		tuneSQLiteWritePool(write)
		tuneSQLiteReadPool(read)
	}
	return newDB(driverName, write, read)
}

// SetSQLiteWithPool 分别指定 SQLite 读写连接池
func SetSQLiteWithPool(write, read *sql.DB) *DB {
	return SetDBWithPool(DriverNameSQLite, write, read)
}

// SetPostgresWithPool 分别指定 PostgreSQL 读写连接池
func SetPostgresWithPool(write, read *sql.DB) *DB {
	return newDB(DriverNamePostgres, write, read)
}

// WriteDB 返回写库句柄
func (d *DB) WriteDB() *sql.DB { return d.DB }

// ReadDB 返回读库句柄；未单独指定时回退到写库
func (d *DB) ReadDB() *sql.DB {
	if d.readDB != nil {
		return d.readDB
	}
	return d.DB
}

// tuneSQLiteWritePool 单写者：写连接串行化，应用层不再与 SQLite 内置锁重试博弈
func tuneSQLiteWritePool(w *sql.DB) {
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
}

// tuneSQLiteReadPool 多读者：绝不与写者共享唯一写连接，读池随 CPU 扩容
func tuneSQLiteReadPool(r *sql.DB) {
	n := max(4, runtime.NumCPU())
	r.SetMaxOpenConns(n)
	r.SetMaxIdleConns(n)
}

// AutoMigrate init database table
// 1. create table nsqite_messages if not exists
// 如果你使用 gorm，可使用 gorm.AutoMigrate(new(nsqite.Message)) 初始化
// 如果你使用 goddd，可使用以下方式
//
// if orm.EnabledAutoMigrate {
// if err := uc.DB.AutoMigrate(new(nsqite.Message)); err != nil {
// panic(err)
// }
// }
func (d *DB) AutoMigrate() error {
	// SQLite 的 WAL 是数据库文件级（持久）属性，开启一次即可跨连接/重启生效。
	// READ/WRITE 并发、且读不阻塞写、写也不阻塞读，是服务端 SQLite 的核心优化之一。
	if d.driverName == DriverNameSQLite {
		if _, err := d.DB.Exec("PRAGMA journal_mode=WAL"); err != nil {
			return err
		}
	}

	var query string
	if d.driverName == DriverNameSQLite {
		query = `
		CREATE TABLE IF NOT EXISTS nsqite_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			topic TEXT NOT NULL DEFAULT '',
			body BLOB NOT NULL,
			consumers INTEGER NOT NULL DEFAULT 0,
			responded INTEGER NOT NULL DEFAULT 0,
			channels TEXT NOT NULL DEFAULT '',
			responded_channels TEXT NOT NULL DEFAULT '',
			attempts INTEGER NOT NULL DEFAULT 0,
			pending INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_messages_consumers_responded ON nsqite_messages (consumers, responded);
		CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON nsqite_messages (timestamp);
		CREATE INDEX IF NOT EXISTS idx_messages_pending ON nsqite_messages (pending, id);
		`
	} else {
		query = `
		CREATE TABLE IF NOT EXISTS nsqite_messages (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			topic TEXT NOT NULL DEFAULT '',
			body BYTEA NOT NULL,
			consumers INTEGER NOT NULL DEFAULT 0,
			responded INTEGER NOT NULL DEFAULT 0,
			channels TEXT NOT NULL DEFAULT '',
			responded_channels TEXT NOT NULL DEFAULT '',
			attempts INTEGER NOT NULL DEFAULT 0,
			pending INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_messages_consumers_responded ON nsqite_messages (consumers, responded);
		CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON nsqite_messages (timestamp);
		CREATE INDEX IF NOT EXISTS idx_messages_pending ON nsqite_messages (pending, id);
		`
	}
	_, err := d.DB.Exec(query)
	return err
}

func getDB() *DB {
	if db == nil {
		panic("please use nsqite.SetDB() to set db")
	}
	return db
}

func (d *DB) Create(value *Message) error {
	if d.driverName == DriverNamePostgres {
		// PostgreSQL 不支持 LastInsertId，需要使用 RETURNING 子句
		query := `INSERT INTO nsqite_messages (topic, body, channels, consumers, responded, responded_channels, timestamp, pending)
		          VALUES ($1, $2, $3, $4, $5, $6, $7, CASE WHEN $5 < $4 THEN 1 ELSE 0 END) RETURNING id`
		err := d.DB.QueryRow(query,
			value.Topic,
			value.Body,
			value.Channels,
			value.Consumers,
			value.Responded,
			value.RespondedChannels,
			value.Timestamp,
		).Scan(&value.ID)
		return err
	}

	query := `INSERT INTO nsqite_messages (topic, body, channels, consumers, responded, responded_channels, timestamp, pending)
	          VALUES (?, ?, ?, ?, ?, ?, ?, CASE WHEN ? < ? THEN 1 ELSE 0 END)`
	result, err := d.DB.Exec(query,
		value.Topic,
		value.Body,
		value.Channels,
		value.Consumers,
		value.Responded,
		value.RespondedChannels,
		value.Timestamp,
		value.Responded,
		value.Consumers,
	)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	value.ID = int(id)
	return nil
}

func (d *DB) DeleteOldMessages(days int) error {
	query := `DELETE FROM nsqite_messages WHERE timestamp < ` + d.placeholder(1)
	thresholdDate := time.Now().AddDate(0, 0, days)
	_, err := d.DB.Exec(query, thresholdDate)
	return err
}

func (d *DB) DeleteCompletedMessagesOlderThan(days int) error {
	query := `DELETE FROM nsqite_messages WHERE responded >= consumers AND timestamp < ` + d.placeholder(1)
	thresholdDate := time.Now().AddDate(0, 0, days)
	_, err := d.DB.Exec(query, thresholdDate)
	return err
}

func (d *DB) FetchPendingMessages(id int, msgs *[]Message) error {
	query := `SELECT id, topic, body, channels, consumers, responded, responded_channels, timestamp, attempts
	          FROM nsqite_messages
	          WHERE id > ` + d.placeholder(1) + ` AND pending = 1
	          ORDER BY id ASC
	          LIMIT 100`
	rows, err := d.ReadDB().Query(query, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.Topic, &msg.Body, &msg.Channels, &msg.Consumers, &msg.Responded, &msg.RespondedChannels, &msg.Timestamp, &msg.Attempts); err != nil {
			return err
		}
		*msgs = append(*msgs, msg)
	}
	return rows.Err()
}

func (d *DB) DriverName() string {
	return d.driverName
}

// placeholder 根据数据库驱动类型返回对应的占位符
// SQLite 使用 ? 占位符，PostgreSQL 使用 $1, $2, $3... 位置参数
func (d *DB) placeholder(index int) string {
	if d.driverName == DriverNamePostgres {
		return fmt.Sprintf("$%d", index)
	}
	return "?"
}

// placeholders 生成多个占位符，用逗号分隔
// 用于 INSERT 语句的 VALUES 部分
func (d *DB) placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	if d.driverName == DriverNamePostgres {
		result := ""
		for i := 1; i <= count; i++ {
			if i > 1 {
				result += ", "
			}
			result += fmt.Sprintf("$%d", i)
		}
		return result
	}
	result := "?"
	for i := 1; i < count; i++ {
		result += ", ?"
	}
	return result
}

func (d *DB) Count() (int, error) {
	var count int
	// 为保证在任意 schema 管理方式（含 GORM/受管库）下始终精确，直接全表计数，
	// 不依赖触发器或预计算表。Count 非热路径，扫描可接受。
	err := d.ReadDB().QueryRow(`SELECT COUNT(id) FROM nsqite_messages`).Scan(&count)
	return count, err
}

// Fold 在一个事务里把一批 WAL op 折入 SQLite（幂等）：
//   - Publish → INSERT OR IGNORE 整行（显式应用侧 id）
//   - Respond → 仅当 channel 未记录时追加并累加 responded
func (d *DB) Fold(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	for _, op := range ops {
		if op.Publish != nil {
			p := op.Publish
			if _, err = tx.Exec(`INSERT OR IGNORE INTO nsqite_messages
				(id, topic, body, timestamp, channels, consumers, responded, responded_channels, attempts, pending)
				VALUES (?, ?, ?, ?, ?, ?, 0, '', ?, CASE WHEN ? > 0 THEN 1 ELSE 0 END)`,
				p.ID, p.Topic, p.Body, time.UnixMilli(p.Timestamp), p.Channels, p.Consumers, p.Attempts, p.Consumers); err != nil {
				return err
			}
		} else if op.Respond != nil {
			r := op.Respond
			if _, err = tx.Exec(`UPDATE nsqite_messages
				SET responded = responded + 1,
				    pending = CASE WHEN responded + 1 >= consumers THEN 0 ELSE 1 END,
				    responded_channels = CASE WHEN responded_channels = '' THEN ?
				                              ELSE responded_channels || ',' || ? END
				WHERE id = ? AND (responded_channels = '' OR (',' || responded_channels || ',') NOT LIKE '%,' || ? || ',%')`,
				r.Channel, r.Channel, r.ID, r.Channel); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
