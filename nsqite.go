package nsqite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxRetentionDays 定义消息的默认最大保留天数为7天
// 超过此天数的消息将被自动清理
const DefaultMaxRetentionDays = 7

// DefaultMaxMessageRows 定义消息表的默认最大行数
// 当消息表行数超过此值时，将仅保留最近3天的消息
const DefaultMaxMessageRows = 10000

var transactionMQ *NSQite
var (
	once          sync.Once
	ErrNoConsumer = errors.New("need consumers")
)

func TransactionMQ() *NSQite {
	once.Do(func() {
		transactionMQ = newNSQite(getDB())
	})
	return transactionMQ
}

// walPath 是 AppWAL 文件路径。设置后 plain Publish 走 WAL-first + 批量折入；为空则走原有的直接落库。
var walPath string

// SetWALPath 配置 AppWAL 文件路径。必须在首次 TransactionMQ() 之前调用。
func SetWALPath(path string) {
	walPath = path
}

// 批量折入参数
const (
	flushBatchThreshold = 512                    // 达到该条数即触发折入
	flushInterval       = 5 * time.Millisecond   // 兜底时间窗
	maxWALBytes         = 64 << 20               // WAL 超过该大小后压缩
)

// NSQite 是消息队列的核心结构
type NSQite struct {
	consumers map[string]*consumerMap
	m         sync.RWMutex

	db   *DB
	wal  *AppWAL
	exit chan struct{}
	once sync.Once

	idSeq atomic.Int64 // 应用侧自增 ID 分配器

	// WAL 模式：内存 responded 态（补偿折入滞后，防重复投递）
	respMu sync.Mutex
	resp   map[int64]map[string]struct{}

	pendingOps  atomic.Int64 // 尚未折入的 op 数
	flushSignal chan struct{}
	catchUp     chan struct{} // 消费者变更/启动触发重放
}

type consumerMap struct {
	m    sync.RWMutex
	data map[string]*Consumer
}

func (c *consumerMap) add(channel string, consumer *Consumer) {
	c.m.Lock()
	defer c.m.Unlock()
	c.data[channel] = consumer
}

func (c *consumerMap) del(channel string) {
	c.m.Lock()
	defer c.m.Unlock()
	delete(c.data, channel)
}

func (c *consumerMap) Len() int {
	c.m.RLock()
	defer c.m.RUnlock()
	return len(c.data)
}

func (c *consumerMap) Channels() (string, uint32) {
	c.m.RLock()
	defer c.m.RUnlock()
	chs := make([]string, 0, 8)
	for ch := range c.data {
		chs = append(chs, ch)
	}
	return strings.Join(chs, ","), uint32(len(chs))
}

func (c *consumerMap) pub(ctx context.Context, msg *Message) error {
	c.m.RLock()
	defer c.m.RUnlock()

	var i int
	for _, c := range c.data {
		i++
		m := msg
		if i > 1 {
			mm := *msg
			m = &mm
		}
		if err := c.SendMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (c *consumerMap) pumpPub(chs []string, msg Message) {
	c.m.RLock()
	defer c.m.RUnlock()

	for _, c := range c.data {
		if !slices.Contains(chs, c.channel) {
			// TODO: 若发送阻塞，会导致整个消息泵阻塞
			// 可以增加延迟队列，将发送失败的消息，放到延迟队列里重试，优先处理未阻塞的消费者
			_ = c.sendMessage(msg)
		}
	}
}

// newNSQite 创建一个新的NSQite实例
func newNSQite(db *DB) *NSQite {
	nsqite := &NSQite{
		db:          db,
		exit:        make(chan struct{}),
		consumers:   make(map[string]*consumerMap),
		flushSignal: make(chan struct{}, 1),
		catchUp:     make(chan struct{}, 1),
		resp:        make(map[int64]map[string]struct{}),
	}
	if walPath != "" {
		if err := nsqite.initWAL(); err != nil {
			slog.Error("newNSQite init WAL", "error", err)
		}
	}
	// 启动消息泵
	go nsqite.messagePump()
	if nsqite.wal != nil {
		go nsqite.flusherLoop()
		nsqite.signalCatchUp()
	}
	return nsqite
}

// initWAL 打开 WAL 并以 SQLite 当前 max(id) 作为 ID 分配底
func (n *NSQite) initWAL() error {
	w, err := OpenWAL(walPath)
	if err != nil {
		return err
	}
	n.wal = w
	return n.seedID()
}

// seedID 以 SQLite 表内 max(id) 抬升应用自增底，避免与 PublishTx 的 AUTOINCREMENT 碰撞
func (n *NSQite) seedID() error {
	var mx int64
	if err := n.db.DB.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM nsqite_messages`).Scan(&mx); err != nil {
		return err
	}
	if mx > n.idSeq.Load() {
		n.idSeq.Store(mx)
	}
	return nil
}

func (n *NSQite) nextID() int {
	return int(n.idSeq.Add(1))
}

// notifyFlush 非阻塞地告知 flusher 有数据待折入
func (n *NSQite) notifyFlush() {
	select {
	case n.flushSignal <- struct{}{}:
	default:
	}
}

func (n *NSQite) signalCatchUp() {
	select {
	case n.catchUp <- struct{}{}:
	default:
	}
}

func (n *NSQite) publishWAL(consumers *consumerMap, msg *Message) error {
	msg.Channels, msg.Consumers = consumers.Channels()
	if msg.Channels == " " {
		return fmt.Errorf("topic %s %w", msg.Topic, ErrNoConsumer)
	}
	msg.ID = n.nextID()
	op := PublishOp{
		ID:        int64(msg.ID),
		Topic:     msg.Topic,
		Body:      msg.Body,
		Timestamp: msg.Timestamp.UnixMilli(),
		Consumers: msg.Consumers,
		Channels:  msg.Channels,
		Attempts:  msg.Attempts,
	}
	if _, err := n.wal.AppendPublish(op); err != nil {
		return err
	}
	n.pendingOps.Add(1)
	n.notifyFlush()
	return consumers.pub(context.Background(), msg)
}

func (n *NSQite) finishWAL(id int64, channel string) error {
	n.respMu.Lock()
	set, ok := n.resp[id]
	if !ok {
		set = make(map[string]struct{})
		n.resp[id] = set
	}
	if _, dup := set[channel]; dup {
		n.respMu.Unlock()
		return nil
	}
	set[channel] = struct{}{}
	n.respMu.Unlock()

	if _, err := n.wal.AppendRespond(RespondOp{ID: id, Channel: channel}); err != nil {
		return err
	}
	n.pendingOps.Add(1)
	n.notifyFlush()
	return nil
}

// flusherLoop 单写者批量折入：把 WAL watermark 之后的 op 一次性折入 SQLite
func (n *NSQite) flusherLoop() {
	n.foldAll()
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()
	for {
		select {
		case <-n.exit:
			n.foldAll()
			return
		case <-n.flushSignal:
			timer.Reset(flushInterval)
			if n.pendingOps.Load() >= flushBatchThreshold {
				n.foldAll()
			}
		case <-timer.C:
			n.foldAll()
			timer.Reset(flushInterval)
		}
	}
}

// foldAll 把 watermark 之后的所有 op 在一个 SQLite 事务里折入，并推进 watermark
func (n *NSQite) foldAll() {
	if n.wal == nil {
		return
	}
	n.pendingOps.Store(0)
	ops, next, err := n.wal.ReadOps(n.wal.Watermark())
	if err != nil {
		slog.Error("foldAll ReadOps", "error", err)
		return
	}
	if len(ops) == 0 {
		return
	}
	if err := n.db.Fold(ops); err != nil {
		slog.Error("foldAll Fold", "error", err)
		return
	}
	if err := n.wal.SetWatermark(next); err != nil {
		slog.Error("foldAll SetWatermark", "error", err)
		return
	}
	_ = n.seedID() // 抬升 ID 底，隔离 PublishTx 的 AUTOINCREMENT
	if n.wal.Size() > maxWALBytes {
		if _, err := n.wal.Compact(); err != nil {
			slog.Error("foldAll Compact", "error", err)
		}
	}
}

// Close 关闭NSQite实例
func (n *NSQite) Close() error {
	n.once.Do(func() {
		close(n.exit)
	})
	return nil
}

func (n *NSQite) consumer(topic string) *consumerMap {
	n.m.RLock()
	consumers, ok := n.consumers[topic]
	n.m.RUnlock()
	if ok {
		return consumers
	}

	n.m.Lock()
	defer n.m.Unlock()
	consumers, ok = n.consumers[topic]
	if ok {
		return consumers
	}

	consumers = &consumerMap{
		data: make(map[string]*Consumer),
	}
	n.consumers[topic] = consumers
	return consumers
}

func (n *NSQite) PublishTx(tx SessionFunc, topic string, msg *Message) error {
	c := n.consumer(topic)
	msg.Channels, msg.Consumers = c.Channels()
	if msg.Channels == " " {
		return fmt.Errorf("topic %s %w", topic, ErrNoConsumer)
	}
	return tx(msg)
}

// Publish 发布消息到Topic
func (n *NSQite) Publish(topic string, msg *Message) error {
	n.m.RLock()
	consumers, ok := n.consumers[topic]
	n.m.RUnlock()
	if !ok {
		return fmt.Errorf("topic %s %w", topic, ErrNoConsumer)
	}

	if n.wal != nil {
		return n.publishWAL(consumers, msg)
	}

	if err := n.PublishTx(n.db.Create, topic, msg); err != nil {
		return err
	}

	return consumers.pub(context.Background(), msg)
}

func (n *NSQite) AddConsumer(c *Consumer) {
	n.m.Lock()
	defer n.m.Unlock()

	consumer, ok := n.consumers[c.topic]
	if !ok {
		consumer = &consumerMap{
			data: make(map[string]*Consumer),
		}
		n.consumers[c.topic] = consumer
	}
	consumer.add(c.channel, c)
	if n.wal != nil {
		n.signalCatchUp()
	}
}

// GetTimeUntilMidnight 返回距离下一个凌晨12点的时间间隔
func GetTimeUntilMidnight() time.Duration {
	now := time.Now()
	tomorrow := now.Add(24 * time.Hour)
	midnight := time.Date(tomorrow.Year(), tomorrow.Month(), tomorrow.Day(), 0, 0, 0, 0, tomorrow.Location())
	return midnight.Sub(now)
}

// messagePump 消息泵，处理消息超时和重试
func (n *NSQite) messagePump() {
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()

	cleanUpTimer := time.NewTimer(GetTimeUntilMidnight())
	defer cleanUpTimer.Stop()

	var id int
	var count int64
	msgs := make([]Message, 0, 24)
	for {
		select {
		case <-n.exit:
			return
		case <-cleanUpTimer.C:
			cleanUpTimer.Reset(GetTimeUntilMidnight())

			if err := n.db.DeleteOldMessages(-15); err != nil {
				slog.Error("messagePump", "error", err)
			}

			if err := n.db.DeleteCompletedMessagesOlderThan(0); err != nil {
				slog.Error("messagePump", "error", err)
				continue
			}
			days := DefaultMaxRetentionDays
			if count > DefaultMaxMessageRows {
				days = 3
			}
			if err := n.db.DeleteCompletedMessagesOlderThan(-days); err != nil {
				slog.Error("messagePump", "error", err)
				continue
			}
		case <-timer.C:
			if n.wal != nil {
				timer.Reset(3 * time.Second)
				continue
			}
			timer.Reset(10 * time.Second)
			msgs = msgs[:0]
			if err := n.db.FetchPendingMessages(id, &msgs); err != nil {
				slog.Error("messagePump", "error", err)
				continue
			}
			if len(msgs) > 0 {
				id = msgs[len(msgs)-1].ID
			}

			for _, msg := range msgs {
				consumers := n.consumer(msg.Topic)
				chs := strings.Split(msg.RespondedChannels, ",")

				consumers.pumpPub(chs, msg)
			}
			timer.Reset(3 * time.Second)
		case <-n.catchUp:
			if n.wal != nil {
				id = 0
				n.drainPending(&id, &msgs)
			}
		}
	}
}

// drainPending 从 SQLite 全量扫描待处理消息并重投；
// WAL 模式下用内存 responded 态过滤，避免折入滞后导致的重复投递
func (n *NSQite) drainPending(id *int, msgs *[]Message) {
	for {
		*msgs = (*msgs)[:0]
		if err := n.db.FetchPendingMessages(*id, msgs); err != nil {
			slog.Error("messagePump drainPending", "error", err)
			return
		}
		if len(*msgs) == 0 {
			return
		}
		*id = (*msgs)[len(*msgs)-1].ID
		for _, msg := range *msgs {
			skip := strings.Split(msg.RespondedChannels, ",")
			if n.wal != nil {
				skip = mergeStrings(skip, n.respondedChannels(int64(msg.ID)))
			}
			n.consumer(msg.Topic).pumpPub(skip, msg)
		}
	}
}

func (n *NSQite) respondedChannels(id int64) []string {
	n.respMu.Lock()
	defer n.respMu.Unlock()
	set, ok := n.resp[id]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(set))
	for ch := range set {
		out = append(out, ch)
	}
	return out
}

func mergeStrings(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		if s != "" {
			seen[s] = struct{}{}
		}
	}
	for _, s := range b {
		if s != "" {
			seen[s] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	return out
}

// finishTx 抽象同一个事务内的查询与执行，SQLite 与 PostgreSQL 共用计算逻辑
type finishTx interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (n *NSQite) Finish(msg *Message, channel string) error {
	if n.wal != nil {
		return n.finishWAL(int64(msg.ID), channel)
	}
	ctx := context.Background()

	var (
		tx   finishTx
		hold func() error
		back func() error
	)
	if n.db.DriverName() == DriverNamePostgres {
		// PostgreSQL 保持原来的串行化事务（FOR UPDATE 行锁）语义
		t, err := n.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		tx, hold, back = t, t.Commit, t.Rollback
	} else {
		// SQLite 必须用 BEGIN IMMEDIATE 打开写锁：
		// 默认 DEFERRED 事务在“读后升级为写”时会立即返回 SQLITE_BUSY，且不遵守 busy_timeout。
		conn, err := n.db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
		tx = conn
		hold = func() error { return commitImmediate(ctx, conn) }
		back = func() error { return rollbackImmediate(ctx, conn) }
	}

	finish := func() error {
		selectQ, updateQ := finishQueries(n.db.DriverName())
		row := tx.QueryRowContext(ctx, selectQ, msg.ID)
		if err := row.Scan(&msg.RespondedChannels, &msg.Responded); err != nil {
			return err
		}

		chs := strings.Split(msg.RespondedChannels, ",")
		for _, ch := range chs {
			if ch == channel {
				return nil
			}
		}
		channels := make([]string, 0, 8)
		for _, ch := range chs {
			if ch != "" {
				channels = append(channels, ch)
			}
		}
		msg.RespondedChannels = strings.Join(append(channels, channel), ",")
		msg.Responded++

		args := []any{msg.RespondedChannels, msg.Responded, msg.Attempts, msg.ID}
		if n.db.DriverName() != DriverNamePostgres {
			args = []any{msg.RespondedChannels, msg.Responded, msg.Attempts, msg.Responded, msg.ID}
		}
		_, err := tx.ExecContext(ctx, updateQ, args...)
		return err
	}

	err := finish()
	if err != nil {
		_ = back()
		return err
	}
	return hold()
}

// finishQueries 根据驱动返回 SELECT/UPDATE 语句（占位符与锁语法不同）
func finishQueries(driverName string) (selectQ, updateQ string) {
	if driverName == DriverNamePostgres {
		selectQ = "SELECT responded_channels, responded FROM nsqite_messages WHERE id = $1 FOR UPDATE"
		updateQ = "UPDATE nsqite_messages SET responded_channels = $1, responded = $2, attempts = $3, " +
			"pending = CASE WHEN $2 >= consumers THEN 0 ELSE 1 END WHERE id = $4"
		return selectQ, updateQ
	}
	selectQ = "SELECT responded_channels, responded FROM nsqite_messages WHERE id = ?"
	updateQ = "UPDATE nsqite_messages SET responded_channels = ?, responded = ?, attempts = ?, " +
		"pending = CASE WHEN ? >= consumers THEN 0 ELSE 1 END WHERE id = ?"
	return selectQ, updateQ
}

func commitImmediate(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "COMMIT")
	return err
}

func rollbackImmediate(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "ROLLBACK")
	return err
}

func (n *NSQite) DelConsumer(topic, channel string) {
	n.m.Lock()
	defer n.m.Unlock()

	chs, ok := n.consumers[topic]
	if !ok {
		return
	}
	chs.del(channel)
	if chs.Len() == 0 {
		delete(n.consumers, topic)
	}
	if n.wal != nil {
		n.signalCatchUp()
	}
}
