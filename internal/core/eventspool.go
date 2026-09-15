package core

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
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

	mu           sync.Mutex
	spillBuf     []Event
	spillPath    string
	drainingPath string

	spilledTotal atomic.Int64
}

func NewEventSpool(capacity int, dir string, sink EventSink) (*EventSpool, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	s := &EventSpool{
		mem:          make(chan Event, capacity),
		sink:         sink,
		dir:          dir,
		spillPath:    filepath.Join(dir, "spool.ndjson.gz"),
		drainingPath: filepath.Join(dir, "spool.draining.ndjson.gz"),
	}

	if err := s.recoverDrainingFile(); err != nil {
		slog.Warn(
			"failed to recover previous draining spool file",
			"error", err,
		)
	}

	if err := s.drainSpillFile(); err != nil {
		slog.Warn(
			"failed to recover previous spool file",
			"error", err,
		)
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
		if count, err := countSpoolEvents(s.spillPath); err == nil {
			pending += count
		} else {
			slog.Warn(
				"failed to count spool events",
				"error", err,
			)

			if fi.Size() > 0 {
				pending++
			}
		}
	}

	if _, err := os.Stat(s.drainingPath); err == nil {
		if count, err := countSpoolEvents(s.drainingPath); err == nil {
			pending += count
		} else {
			pending++
		}
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
				"count", len(batch),
				"error", err,
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
				"count", len(toWrite),
				"error", err,
			)

			s.mu.Lock()
			s.spillBuf = append(toWrite, s.spillBuf...)
			s.mu.Unlock()
		}
	}
}

func (s *EventSpool) appendSpillMember(events []Event) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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
				"error", err,
			)
		}
	}
}

func (s *EventSpool) drainSpillFile() error {
	s.mu.Lock()

	if _, err := os.Stat(s.drainingPath); os.IsNotExist(err) {
		if _, err := os.Stat(s.spillPath); os.IsNotExist(err) {
			s.mu.Unlock()
			return nil
		}

		if err := os.Rename(s.spillPath, s.drainingPath); err != nil {
			s.mu.Unlock()
			return err
		}
	}

	s.mu.Unlock()

	return s.drainFile(s.drainingPath)
}

func (s *EventSpool) drainFile(path string) error {
	events, err := readSpoolEvents(path)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}

	const batchSize = 500

	drained := 0

	for drained < len(events) {
		end := drained + batchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[drained:end]

		if err := s.sink(batch); err != nil {
			if drained == 0 {
				return err
			}

			remaining := events[drained:]

			if rewriteErr := rewriteSpoolFile(path, remaining); rewriteErr != nil {
				return fmt.Errorf(
					"persisted %d events but failed to preserve remaining %d events: %w",
					drained,
					len(remaining),
					rewriteErr,
				)
			}

			return err
		}

		drained = end
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	slog.Info(
		"drained spool file",
		"count", drained,
	)

	return nil
}

func (s *EventSpool) recoverDrainingFile() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := os.Stat(s.drainingPath); os.IsNotExist(err) {
		return nil
	}

	if _, err := os.Stat(s.spillPath); os.IsNotExist(err) {
		return os.Rename(s.drainingPath, s.spillPath)
	}

	recovered, err := readSpoolEvents(s.drainingPath)
	if err != nil {
		return err
	}

	existing, err := readSpoolEvents(s.spillPath)
	if err != nil {
		return err
	}

	combined := make([]Event, 0, len(recovered)+len(existing))
	combined = append(combined, recovered...)
	combined = append(combined, existing...)

	tempPath := s.spillPath + ".recovering"

	if err := writeSpoolFile(tempPath, combined); err != nil {
		return err
	}

	if err := os.Rename(tempPath, s.spillPath); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	if err := os.Remove(s.drainingPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func readSpoolEvents(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	gr.Multistream(true)

	scanner := bufio.NewScanner(gr)
	scanner.Buffer(
		make([]byte, 64*1024),
		1024*1024,
	)

	events := make([]Event, 0)

	for scanner.Scan() {
		var e Event

		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			slog.Warn(
				"skipping corrupt spool event",
				"error", err,
			)
			continue
		}

		events = append(events, e)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if err := gr.Close(); err != nil {
		return nil, err
	}

	return events, nil
}

func writeSpoolFile(path string, events []Event) error {
	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
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

func rewriteSpoolFile(path string, events []Event) error {
	tempPath := path + ".rewrite"

	if err := writeSpoolFile(tempPath, events); err != nil {
		return err
	}

	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	return nil
}

func countSpoolEvents(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gr.Close()

	gr.Multistream(true)

	scanner := bufio.NewScanner(gr)
	scanner.Buffer(
		make([]byte, 64*1024),
		1024*1024,
	)

	count := 0

	for scanner.Scan() {
		count++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return count, nil
}
