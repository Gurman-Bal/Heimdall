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

type EventSpool struct {
	mem  chan Event
	sink EventSink
	dir  string

	mu        sync.Mutex
	spillBuf  []Event
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

	if err := s.drainSpillFile(); err != nil {
		slog.Warn("failed to recover previous spool file", "error", err)
	}

	go s.memDrainLoop()
	go s.spillFlushLoop()
	go s.spillDrainLoop()

	return s, nil
}

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

func (s *EventSpool) SpilledCount() int64 {
	return s.spilledTotal.Load()
}

func (s *EventSpool) BacklogSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending := len(s.spillBuf)

	if fi, err := os.Stat(s.spillPath); err == nil {
		pending += int(fi.Size() / 100)
	}

	return pending
}

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
			slog.Error(
				"failed to persist event batch",
				"count",
				len(batch),
				"error",
				err,
			)
			return
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

func (s *EventSpool) spillFlushLoop() {
	ticker := time.NewTicker(time.Second)
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
			slog.Error(
				"failed to write spill file",
				"count",
				len(toWrite),
				"error",
				err,
			)

			s.mu.Lock()
			s.spillBuf = append(toWrite, s.spillBuf...)
			s.mu.Unlock()
		}
	}
}

func (s *EventSpool) appendSpillMember(events []Event) error {
	f, err := os.OpenFile(
		s.spillPath,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}

	gw := gzip.NewWriter(f)
	enc := json.NewEncoder(gw)

	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			_ = gw.Close()
			_ = f.Close()
			return err
		}
	}

	if err := gw.Close(); err != nil {
		_ = f.Close()
		return err
	}

	return f.Close()
}

func (s *EventSpool) spillDrainLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := s.drainSpillFile(); err != nil {
			slog.Warn(
				"spill drain attempt incomplete, will retry",
				"error",
				err,
			)
		}
	}
}

func (s *EventSpool) drainSpillFile() error {
	s.mu.Lock()

	if _, err := os.Stat(s.spillPath); os.IsNotExist(err) {
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
		_ = f.Close()
		return err
	}

	gr.Multistream(true)

	scanner := bufio.NewScanner(gr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	batch := make([]Event, 0, 500)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := s.sink(batch); err != nil {
			return err
		}

		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		var e Event

		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			slog.Warn("skipping corrupt spool event", "error", err)
			continue
		}

		batch = append(batch, e)

		if len(batch) >= 500 {
			if err := flush(); err != nil {
				_ = gr.Close()
				_ = f.Close()
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		_ = gr.Close()
		_ = f.Close()
		return err
	}

	if err := flush(); err != nil {
		_ = gr.Close()
		_ = f.Close()
		return err
	}

	if err := gr.Close(); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.spillPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	slog.Info("drained spool file", "count", len(batch))

	return nil
}
