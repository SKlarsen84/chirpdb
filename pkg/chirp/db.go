package chirp

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var ErrNotFound = errors.New("not found")

// ----- On-disk record -----
// little-endian header (21 bytes):
// crc32 (4) | ts (8) | klen (4) | vlen (4) | flags (1)
const (
	hdrSize = 21
	flagDel = 1
)

// Options tunes durability/perf.
type Options struct {
	// If >0, enable background writer: fsync once per interval or BatchMax ops.
	FsyncInterval time.Duration
	// Force a flush when this many ops are queued (used only if FsyncInterval>0).
	BatchMax int
	// Buffer size for buffered writer.
	WriterBufferBytes int
	// Threshold that triggers meaningful work in Merge().
	MaxActiveBytes int64
}

func (o *Options) setDefaults() {
	if o.BatchMax <= 0 {
		o.BatchMax = 128
	}
	if o.WriterBufferBytes <= 0 {
		o.WriterBufferBytes = 1 << 20 // 1 MiB
	}
	if o.MaxActiveBytes <= 0 {
		o.MaxActiveBytes = 256 << 20 // 256 MiB
	}
}

// DB is a tiny Bitcask-like KV: single file, append-only, hash index in RAM.
type DB struct {
	// storage
	dir      string
	active   *os.File
	writer   *bufio.Writer
	activeSz int64

	// index
	mu    sync.RWMutex
	index map[string]entry

	// config
	maxActive int64

	// batching
	queue   chan *req
	wg      sync.WaitGroup
	stopOnce sync.Once
}

type entry struct {
	offset int64 // file offset at start of header
	size   int64 // full record size
}

type opKind uint8

const (
	opPut opKind = iota + 1
	opDel
)

type req struct {
	op   opKind
	key  string
	val  []byte
	done chan error
}

// Open returns a DB with synchronous writes (fsync per op).
func Open(dir string) (*DB, error) { return OpenWithOptions(dir, &Options{}) }

// OpenWithOptions enables batched fsync when FsyncInterval>0.
func OpenWithOptions(dir string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	opts.setDefaults()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "data.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dir:       dir,
		active:    f,
		writer:    bufio.NewWriterSize(f, opts.WriterBufferBytes),
		index:     make(map[string]entry, 1024),
		maxActive: opts.MaxActiveBytes,
	}
	if err := db.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}

	if opts.FsyncInterval > 0 {
		db.queue = make(chan *req, opts.BatchMax*4)
		db.wg.Add(1)
		go db.runWriter(opts.FsyncInterval, opts.BatchMax)
	}

	return db, nil
}

func (db *DB) Close() error {
	if db.queue != nil {
		db.stopOnce.Do(func() { close(db.queue) })
		db.wg.Wait()
	}

	db.mu.Lock()
	if db.writer != nil {
		_ = db.writer.Flush()
	}
	if db.active != nil {
		_ = db.active.Sync()
	}
	f := db.active
	db.active = nil
	db.writer = nil
	db.mu.Unlock()

	if f != nil {
		return f.Close()
	}
	return nil
}

// Get returns a copy of the value for key.
func (db *DB) Get(key string) ([]byte, error) {
	db.mu.RLock()
	ent, ok := db.index[key]
	f := db.active
	db.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	h, kb, vb, err := readAt(f, ent.offset)
	if err != nil {
		return nil, err
	}
	if h.flags&flagDel != 0 || string(kb) != key {
		return nil, ErrNotFound
	}
	out := make([]byte, len(vb))
	copy(out, vb)
	return out, nil
}

// Put stores key->val.
func (db *DB) Put(key string, val []byte) error {
	if db.queue == nil {
		db.mu.Lock()
		err := db.appendLocked(key, val, false, true)
		db.mu.Unlock()
		return err
	}
	return db.enqueue(opPut, key, val)
}

// Delete removes key (idempotent).
func (db *DB) Delete(key string) error {
	if db.queue == nil {
		db.mu.Lock()
		err := db.appendLocked(key, nil, true, true)
		db.mu.Unlock()
		return err
	}
	return db.enqueue(opDel, key, nil)
}

// Merge compacts live keys into a fresh file atomically.
func (db *DB) Merge() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeSz < db.maxActive {
		return nil
	}

	tmp := filepath.Join(db.dir, "data.compacting")
	out, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	w := bufio.NewWriterSize(out, 1<<20)
	newIndex := make(map[string]entry, len(db.index))

	for k, ent := range db.index {
		h, kb, vb, err := readAt(db.active, ent.offset)
		if err != nil {
			return err
		}
		if h.flags&flagDel != 0 || string(kb) != k {
			continue
		}
		rec := encodeRecord(kb, vb, false)
		off, _ := out.Seek(0, io.SeekEnd)
		if _, err := w.Write(rec); err != nil {
			return err
		}
		newIndex[k] = entry{offset: off, size: int64(len(rec))}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}

	old := filepath.Join(db.dir, "data.log")
	bak := filepath.Join(db.dir, "data.old")
	_ = os.Remove(bak)
	if err := os.Rename(old, bak); err != nil {
		return err
	}
	if err := os.Rename(tmp, old); err != nil {
		_ = os.Rename(bak, old)
		return err
	}
	_ = os.Remove(bak)

	_ = db.active.Close()
	f, err := os.OpenFile(old, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	db.active = f
	db.writer = bufio.NewWriterSize(f, 1<<20)
	db.index = newIndex
	fi, _ := f.Stat()
	db.activeSz = fi.Size()
	return nil
}

// Keys returns a snapshot of current keys (unsafe across concurrent writes).
func (db *DB) Keys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]string, 0, len(db.index))
	for k := range db.index {
		out = append(out, k)
	}
	return out
}

// QueryResult represents a single query result
type QueryResult struct {
	Key   string
	Value map[string]interface{}
}

// Query executes a SQL-like query on JSON documents in the database
func (db *DB) Query(queryStr string, params map[string]string) ([]QueryResult, error) {
	query, err := ParseQuery(queryStr, params)
	if err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	var results []QueryResult
	f := db.active

	for key, ent := range db.index {
		h, kb, vb, err := readAt(f, ent.offset)
		if err != nil {
			continue // Skip corrupted entries
		}
		if h.flags&flagDel != 0 || string(kb) != key {
			continue // Skip deleted entries
		}

		// Try to parse as JSON
		trim := strings.TrimLeftFunc(string(vb), func(r rune) bool { return r <= ' ' })
		if !strings.HasPrefix(trim, "{") {
			continue // Skip non-JSON object values (arrays and primitives)
		}

		var doc map[string]interface{}
		if err := json.Unmarshal(vb, &doc); err != nil {
			continue // Skip invalid JSON
		}

		// Check if document matches query conditions
		if query.Match(doc) {
			// Project fields if specified
			projected := query.Project(doc)
			results = append(results, QueryResult{
				Key:   key,
				Value: projected,
			})
		}
	}

	return results, nil
}

// ----- internal batching -----

func (db *DB) enqueue(op opKind, key string, val []byte) error {
	done := make(chan error, 1)
	db.queue <- &req{op: op, key: key, val: val, done: done}
	return <-done
}

func (db *DB) runWriter(interval time.Duration, batchMax int) {
	defer db.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batch := make([]*req, 0, batchMax)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		db.mu.Lock()
		for _, r := range batch {
			switch r.op {
			case opPut:
				_ = db.appendLocked(r.key, r.val, false, false)
			case opDel:
				_ = db.appendLocked(r.key, nil, true, false)
			}
		}
		_ = db.writer.Flush()
		_ = db.active.Sync()
		db.mu.Unlock()
		for _, r := range batch {
			r.done <- nil
		}
		batch = batch[:0]
	}

	for {
		select {
		case r, ok := <-db.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, r)
			if len(batch) >= batchMax {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// appendLocked writes a record; caller must hold db.mu.
// If syncNow is true, Flush+Sync happens inside.
func (db *DB) appendLocked(key string, val []byte, del bool, syncNow bool) error {
	kb := []byte(key)
	rec := encodeRecord(kb, val, del)

	off, err := db.active.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := db.writer.Write(rec); err != nil {
		return err
	}
	if syncNow {
		if err := db.writer.Flush(); err != nil {
			return err
		}
		if err := db.active.Sync(); err != nil {
			return err
		}
	}

	if del {
		delete(db.index, key)
	} else {
		db.index[key] = entry{offset: off, size: int64(len(rec))}
	}
	db.activeSz = off + int64(len(rec))
	return nil
}

// ----- replay & codec -----

type recHeader struct {
	crc   uint32
	ts    uint64
	klen  uint32
	vlen  uint32
	flags uint8
}

func encodeRecord(k, v []byte, del bool) []byte {
	h := recHeader{
		ts:    uint64(time.Now().UnixNano()),
		klen:  uint32(len(k)),
		vlen:  uint32(len(v)),
	}
	if del {
		h.flags = flagDel
	}
	buf := make([]byte, hdrSize+len(k)+len(v))
	writeHeader(buf[:hdrSize], h, 0)
	copy(buf[hdrSize:], k)
	copy(buf[hdrSize+len(k):], v)
	crc := crc32.ChecksumIEEE(buf[4:])
	writeHeader(buf[:hdrSize], h, crc)
	return buf
}

func readAt(f *os.File, off int64) (recHeader, []byte, []byte, error) {
	hb := make([]byte, hdrSize)
	if _, err := f.ReadAt(hb, off); err != nil {
		return recHeader{}, nil, nil, err
	}
	h := parseHeader(hb)
	body := make([]byte, h.klen+h.vlen)
	if _, err := f.ReadAt(body, off+hdrSize); err != nil {
		return recHeader{}, nil, nil, err
	}
	if crc32.ChecksumIEEE(append(hb[4:], body...)) != h.crc {
		return recHeader{}, nil, nil, io.ErrUnexpectedEOF
	}
	return h, body[:h.klen], body[h.klen:], nil
}

func writeHeader(dst []byte, h recHeader, crc uint32) {
	binary.LittleEndian.PutUint32(dst[0:4], crc)
	binary.LittleEndian.PutUint64(dst[4:12], h.ts)
	binary.LittleEndian.PutUint32(dst[12:16], h.klen)
	binary.LittleEndian.PutUint32(dst[16:20], h.vlen)
	dst[20] = h.flags
}

func parseHeader(src []byte) recHeader {
	return recHeader{
		crc:   binary.LittleEndian.Uint32(src[0:4]),
		ts:    binary.LittleEndian.Uint64(src[4:12]),
		klen:  binary.LittleEndian.Uint32(src[12:16]),
		vlen:  binary.LittleEndian.Uint32(src[16:20]),
		flags: src[20],
	}
}

// replay scans the file, builds the index, truncates a torn tail if found.
func (db *DB) replay() error {
	var off int64 = 0
	r := io.NewSectionReader(db.active, 0, 1<<62)

	for {
		hb := make([]byte, hdrSize)
		_, err := r.ReadAt(hb, off)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}
		h := parseHeader(hb)
		if h.klen > 1<<20 || h.vlen > 1<<31 {
			break // sanity
		}
		recLen := int64(hdrSize) + int64(h.klen+h.vlen)
		body := make([]byte, h.klen+h.vlen)
		if _, err := r.ReadAt(body, off+hdrSize); err != nil {
			break
		}
		if crc32.ChecksumIEEE(append(hb[4:], body...)) != h.crc {
			break // torn tail
		}
		key := string(body[:h.klen])
		if h.flags&flagDel != 0 {
			delete(db.index, key)
		} else {
			db.index[key] = entry{offset: off, size: recLen}
		}
		off += recLen
	}
	db.activeSz = off
	if _, err := db.active.Seek(off, io.SeekStart); err == nil {
		_ = db.active.Truncate(off)
	}
	db.writer = bufio.NewWriterSize(db.active, 1<<20)
	return nil
}
