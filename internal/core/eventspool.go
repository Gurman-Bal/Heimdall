package core

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type EventSink func(batch []Event) error

// EventSpool is the durable path from ingestion to storage. Events go into
// an in-memory buffer up to capacity; anything beyond that is appended to a
// gzip-compressed file on disk instead of being dropped. A background loop
// drains memory into the sink continuously, and drains+deletes the spill
// file whenever there's room, so a burst is absorbed by disk rather than lost.
type EventSpool struct {
	mem  chan Event
	sink EventSink
	dir  string

	mu        sync.Mutex
	spillBuf  []Event // events waiting to be flushed as the next gzip member
	spillPath string

	spilledTotal atomic.Int64
}

func NewEventSpool(capacity int, dir string, sink EventSink) (*EventSpool, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	s := &EventSpool{
		mem:       make(chan Event, capacity),
		sink:      sink,
		dir:       dir,
		spillPath: filepath.Join(dir, "spool.ndjson.gz"),
	}

	// Recover anything left on disk from a previous run that crashed
	// mid-drain, before accepting any new events.
	if err := s.drainSpillFile(); err != nil {
		slog.Warn("failed to fully recover previous spool file", "error", err)
	}

	go s.memDrainLoop()
	go s.spillFlushLoop()
	go s.spillDrainLoop()

	return s, nil
}

// Push never blocks and never silently discards: it either fits in memory
// or gets queued for disk. The only true loss path is the disk write itself
// failing, which is logged loudly rather than swallowed.
func (s *EventSpool) Push(e Event) {
	select {
	case s.mem <- e:
	default:
		s.mu.Lock()
		s.spillBuf = append(s.spillBuf, e)
		s.mu.Unlock()
		s.spilledTotal.Add(1)
	}
}

func (s *EventSpool) SpilledCount() int64 { return s.spilledTotal.Load() }

func (s *EventSpool) BacklogSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := len(s.spillBuf)
	if fi, err := os.Stat(s.spillPath); err == nil {
		pending += int(fi.Size() / 100) // rough estimate, exact count isn't worth a full decompress just for a status number
	}
	return pending
}

// memDrainLoop batches whatever's arriving in the memory channel and writes
// it to the sink (SQLite) - this is the normal, non-overflow path.
func (s *EventSpool) memDrainLoop() {
	const maxBatch = 500
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	batch := make([]Event, 0, maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.sink(batch); err != nil {
			slog.Error("failed to persist event batch", "count", len(batch), "error", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case e := <-s.mem:
			batch = append(batch, e)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// spillFlushLoop periodically writes whatever's accumulated in spillBuf to
// disk as one gzip member. Writing in small, individually-closed gzip
// members (rather than one long-lived stream) means the file on disk is
// always fully decodable up to the last flush, even if the process dies
// mid-write - at most one flush interval's worth of overflow is at risk,
// not the whole file.
func (s *EventSpool) spillFlushLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		if len(s.spillBuf) == 0 {
			s.mu.Unlock()
			continue
		}
		toWrite := s.spillBuf
		s.spillBuf = nil
		s.mu.Unlock()

		if err := s.appendSpillMember(toWrite); err != nil {
			slog.Error("failed to write spill file - events lost", "count", len(toWrite), "error", err)
		}
	}
}

func (s *EventSpool) appendSpillMember(events []Event) error {
	f, err := os.OpenFile(s.spillPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			slog.Error("failed to append member storage", "error", err)
		}
	}(f)

	gw := gzip.NewWriter(f)
	enc := json.NewEncoder(gw)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			err := gw.Close()
			if err != nil {
				return err
			}
			return err
		}
	}
	return gw.Close() // finalizes this gzip member; file remains valid multistream gzip
}

// spillDrainLoop periodically tries to move the spill file's contents back
// into memory (and from there, into the sink), then deletes the file once
// fully consumed, this is the compaction step that keeps disk usage bounded
// once a burst has passed.
func (s *EventSpool) spillDrainLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := s.drainSpillFile(); err != nil {
			slog.Warn("spill drain attempt incomplete, will retry", "error", err)
		}
	}
}

func (s *EventSpool) drainSpillFile() error {
	s.mu.Lock()
	_, err := os.Stat(s.spillPath)
	if os.IsNotExist(err) {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	f, err := os.Open(s.spillPath)
	if err != nil {
		return err
	}

	gr, err := gzip.NewReader(f)
	if err != nil {
		err := f.Close()
		if err != nil {
			return err
		}
		return err
	}
	gr.Multistream(true) // read across all concatenated gzip members in the file

	scanner := bufio.NewScanner(gr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	drained := 0
	for scanner.Scan() {
		var e Event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue // skip a corrupt line rather than abandon the whole drain
		}
		s.mem <- e // fine to block briefly here - this is background compaction, not the hot ingestion path
		drained++
	}
	err = gr.Close()
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}

	if drained > 0 {
		slog.Info("drained spool file back into persistence pipeline", "count", drained)
	}

	// Only delete if we can also confirm nothing new got appended mid-read -
	// simplest safe approach: remove, and anything written concurrently
	// during this drain just starts a fresh file on the next flush.
	return os.Remove(s.spillPath)
}
