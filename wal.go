package nsqite

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// WAL 持久化配置（类比 TDengine 的 wal_level / wal_fsync_period）。
//   - wal_level: 1 = 只写 WAL 不 fsync；2 = 执行 fsync。默认 2。
//   - wal_fsync_period: 当 wal_level=2 时的 fsync 周期；0 = 每次写入立即 fsync。默认 0。
var (
	defaultWALLevel       int           = 2
	defaultWALFsyncPeriod time.Duration = 0
)

// SetWALLevel 设置 WAL 持久化级别：1 不 fsync，2 执行 fsync
func SetWALLevel(level int) { defaultWALLevel = level }

// SetWALFsyncPeriod 设置 fsync 周期；0 表示每次写入立即 fsync，>0 表示按周期批量 fsync
func SetWALFsyncPeriod(d time.Duration) { defaultWALFsyncPeriod = d }

// OpType 标识 AppWAL 中的操作类型
type OpType byte

const (
	OpPublish OpType = 1 // 写入一条新消息
	OpRespond OpType = 2 // 一个 channel 完成处理
)

// 帧头固定 5 字节：[1B type][4B big-endian payloadLen]
const recordHeaderLen = 5

// Op 是由 AppWAL 读出的一个操作
type Op struct {
	Type    OpType
	Publish *PublishOp
	Respond *RespondOp
}

// PublishOp 对应 OpPublish，payload 携带完整消息内容（含 body）以便折入 SQLite
type PublishOp struct {
	ID        int64
	Topic     string
	Body      []byte
	Timestamp int64 // unix 毫秒
	Consumers uint32
	Channels  string
	Attempts  uint32
}

// RespondOp 对应 OpRespond，记录某个 channel 完成处理
type RespondOp struct {
	ID      int64
	Channel string
}

// AppWAL 是“先写日志、再批量折入 SQLite”的持久化前置。
// 追加写与读/压缩受同一把锁保护；fsync 在每条 append 后执行（严格持久）。
type AppWAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
	eof  int64  // 当前文件末尾字节偏移
	wm   int64  // watermark：SQLite 已折入到该偏移
	wmF  string // watermark 持久化文件

	level       int           // 1 或 2
	fsyncPeriod time.Duration // 0 = 每条 fsync；>0 = 按周期
	dirty       bool
	done        chan struct{}
}

// OpenWAL 打开（或创建）WAL 及其 watermark 文件
func OpenWAL(path string) (*AppWAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	wmF := path + ".wm"
	wm, err := loadInt(wmF)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	w := &AppWAL{
		f: f, path: path, eof: st.Size(), wm: wm, wmF: wmF,
		level: defaultWALLevel, fsyncPeriod: defaultWALFsyncPeriod, done: make(chan struct{}),
	}
	if w.level == 2 && w.fsyncPeriod > 0 {
		go w.fsyncLoop()
	}
	return w, nil
}

// AppendPublish 追加一条发布操作，fsync 后返回记录末尾偏移
func (w *AppWAL) AppendPublish(op PublishOp) (int64, error) {
	return w.appendRecord(encodePublish(op))
}

// AppendRespond 追加一条响应操作，fsync 后返回记录末尾偏移
func (w *AppWAL) AppendRespond(op RespondOp) (int64, error) {
	return w.appendRecord(encodeRespond(op))
}

func (w *AppWAL) appendRecord(rec []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var hdr [recordHeaderLen]byte
	hdr[0] = rec[0]
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(rec)-1))

	off := w.eof
	if _, err := w.f.WriteAt(hdr[:], off); err != nil {
		return 0, err
	}
	if _, err := w.f.WriteAt(rec[1:], off+recordHeaderLen); err != nil {
		return 0, err
	}
	switch {
	case w.level == 1:
		// 只写 WAL，不执行 fsync（依赖 OS 缓存，关闭/崩溃时尾部可能丢失）
	case w.fsyncPeriod == 0:
		// wal_level=2 + period=0：每条写入立即 fsync，严格持久
		if err := w.f.Sync(); err != nil {
			return 0, err
		}
	default:
		// wal_level=2 + period>0：由后台周期批量 fsync（组提交）
		w.dirty = true
	}
	w.eof = off + recordHeaderLen + int64(len(rec)-1)
	return w.eof, nil
}

// fsyncLoop 组提交：按 fsyncPeriod 周期把脏页落盘
func (w *AppWAL) fsyncLoop() {
	t := time.NewTicker(w.fsyncPeriod)
	defer t.Stop()
	for {
		select {
		case <-w.done:
			w.syncDirty()
			return
		case <-t.C:
			w.syncDirty()
		}
	}
}

func (w *AppWAL) syncDirty() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dirty {
		_ = w.f.Sync()
		w.dirty = false
	}
}

// ReadOps 从 offset（必须是记录边界）读到当前 EOF，返回解析出的操作与下一边界
func (w *AppWAL) ReadOps(offset int64) ([]Op, int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if offset > w.eof {
		return nil, w.eof, nil
	}
	buf := make([]byte, w.eof-offset)
	n, err := w.f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, offset, err
	}
	if n < len(buf) {
		buf = buf[:n]
	}
	ops, consumed := parseOps(buf)
	return ops, offset + int64(consumed), nil
}

// Watermark 返回当前已折入偏移
func (w *AppWAL) Watermark() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.wm
}

// Size 返回当前文件末尾偏移（用于决定是否压缩）
func (w *AppWAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.eof
}

// SetWatermark 持久化推进 watermark
func (w *AppWAL) SetWatermark(off int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := persistInt(w.wmF, off); err != nil {
		return err
	}
	w.wm = off
	return nil
}

// Compact 重写 WAL，丢弃 ≤watermark 的区段（SQLite 已折入），并把 watermark 重置为 0。
// 返回压缩后的文件末尾偏移；调用方应将本地读游标重置为 0。
func (w *AppWAL) Compact() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	keep := w.wm
	if keep > w.eof {
		keep = w.eof
	}
	tail := make([]byte, w.eof-keep)
	if _, err := w.f.ReadAt(tail, keep); err != nil && len(tail) > 0 {
		return 0, err
	}
	if err := w.f.Truncate(0); err != nil {
		return 0, err
	}
	if _, err := w.f.WriteAt(tail, 0); err != nil {
		return 0, err
	}
	if err := w.f.Sync(); err != nil {
		return 0, err
	}
	if err := persistInt(w.wmF, 0); err != nil {
		return 0, err
	}
	w.eof = int64(len(tail))
	w.wm = 0
	return w.eof, nil
}

// Close 关闭 WAL
func (w *AppWAL) Close() error {
	w.mu.Lock()
	close(w.done)
	if w.dirty {
		_ = w.f.Sync()
	}
	err := w.f.Close()
	w.mu.Unlock()
	return err
}

func encodePublish(op PublishOp) []byte {
	payload := make([]byte, 0, 8+2+len(op.Topic)+4+len(op.Body)+8+4+2+len(op.Channels)+4)
	payload = appendInt64(payload, op.ID)
	payload = appendString(payload, op.Topic)
	payload = appendBytes(payload, op.Body)
	payload = appendInt64(payload, op.Timestamp)
	payload = appendUint32(payload, op.Consumers)
	payload = appendString(payload, op.Channels)
	payload = appendUint32(payload, op.Attempts)
	rec := make([]byte, 1+len(payload))
	rec[0] = byte(OpPublish)
	copy(rec[1:], payload)
	return rec
}

func encodeRespond(op RespondOp) []byte {
	payload := make([]byte, 0, 8+2+len(op.Channel))
	payload = appendInt64(payload, op.ID)
	payload = appendString(payload, op.Channel)
	rec := make([]byte, 1+len(payload))
	rec[0] = byte(OpRespond)
	copy(rec[1:], payload)
	return rec
}

// parseOps 解析从记录边界开始的一段字节；遇到残缺记录即停止
func parseOps(buf []byte) (ops []Op, consumed int) {
	off := 0
	for off+recordHeaderLen <= len(buf) {
		typ := OpType(buf[off])
		plen := int(binary.BigEndian.Uint32(buf[off+1 : off+recordHeaderLen]))
		if off+recordHeaderLen+plen > len(buf) {
			break
		}
		payload := buf[off+recordHeaderLen : off+recordHeaderLen+plen]
		var op Op
		switch typ {
		case OpPublish:
			op.Type = OpPublish
			op.Publish = decodePublish(payload)
		case OpRespond:
			op.Type = OpRespond
			op.Respond = decodeRespond(payload)
		default:
			return ops, off
		}
		ops = append(ops, op)
		off += recordHeaderLen + plen
	}
	return ops, off
}

func decodePublish(p []byte) *PublishOp {
	var op PublishOp
	op.ID, p = takeInt64(p)
	op.Topic, p = takeString(p)
	op.Body, p = takeBytes(p)
	op.Timestamp, p = takeInt64(p)
	op.Consumers, p = takeUint32(p)
	op.Channels, p = takeString(p)
	op.Attempts, _ = takeUint32(p)
	return &op
}

func decodeRespond(p []byte) *RespondOp {
	var op RespondOp
	op.ID, p = takeInt64(p)
	op.Channel, _ = takeString(p)
	return &op
}

func appendInt64(b []byte, v int64) []byte { return binary.BigEndian.AppendUint64(b, uint64(v)) }
func appendUint32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}
func appendString(b []byte, s string) []byte {
	b = appendUint32(b, uint32(len(s)))
	return append(b, s...)
}
func appendBytes(b []byte, v []byte) []byte {
	b = appendUint32(b, uint32(len(v)))
	return append(b, v...)
}

func takeInt64(b []byte) (int64, []byte) {
	if len(b) < 8 {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(b)), b[8:]
}
func takeUint32(b []byte) (uint32, []byte) {
	if len(b) < 4 {
		return 0, nil
	}
	return binary.BigEndian.Uint32(b), b[4:]
}
func takeString(b []byte) (string, []byte) {
	n, b := takeUint32(b)
	if uint64(len(b)) < uint64(n) {
		return "", nil
	}
	return string(b[:n]), b[n:]
}
func takeBytes(b []byte) ([]byte, []byte) {
	n, b := takeUint32(b)
	if uint64(len(b)) < uint64(n) {
		return nil, nil
	}
	return b[:n], b[n:]
}

func loadInt(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}

func persistInt(path string, v int64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	return os.WriteFile(path, b[:], 0o644)
}
