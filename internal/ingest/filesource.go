package ingest

import (
	"bufio"
	"log/slog"
	"os"
	"sync"

	"heimdall/internal/core"
)

type OffsetStore interface {
	GetOffset(source, path string) (int64, bool, error)
	SetOffset(source, path string, offset int64) error
}

type Classifier interface {
	Classify(sourceType, message string) (severity, eventType string)
}

// ParseFunc converts one raw log line into an Event. This is the only thing
// that differs between source types - everything else is shared.
type ParseFunc func(line string) core.Event

type fileState struct {
	path   string
	offset int64
}

// FileSource is a generic incremental file tailer. Any plugin backed by
// plain-text log files (TrueNAS, Minecraft, Docker, whatever comes next)
// reuses this instead of reimplementing tailing/offsets/rotation handling.
type FileSource struct {
	sourceType string
	parse      ParseFunc
	store      OffsetStore
	classifier Classifier // may be nil - falls back to whatever parse() set

	mu     sync.Mutex
	states []*fileState
}

func NewFileSource(sourceType string, paths []string, parse ParseFunc, store OffsetStore, classifier Classifier) *FileSource {
	f := &FileSource{sourceType: sourceType, parse: parse, store: store, classifier: classifier}
	for _, p := range paths {
		f.states = append(f.states, &fileState{path: p})
	}
	return f
}

func (f *FileSource) Name() string { return f.sourceType }

func (f *FileSource) Start() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, st := range f.states {
		f.seedOffset(st)
		slog.Info("tailing path", "plugin", f.sourceType, "path", st.path, "offset", st.offset)
	}
	return nil
}

func (f *FileSource) seedOffset(st *fileState) {
	if f.store != nil {
		if offset, found, err := f.store.GetOffset(f.sourceType, st.path); err == nil && found {
			st.offset = offset
			return
		}
	}
	if fi, err := os.Stat(st.path); err == nil {
		st.offset = fi.Size()
	}
}

func (f *FileSource) AddPath(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, st := range f.states {
		if st.path == path {
			slog.Warn("path already tracked, ignoring add", "plugin", f.sourceType, "path", path)
			return
		}
	}
	st := &fileState{path: path}
	f.seedOffset(st)
	f.states = append(f.states, st)
	slog.Info("path added", "plugin", f.sourceType, "path", path, "offset", st.offset)
}

func (f *FileSource) RemovePath(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, st := range f.states {
		if st.path == path {
			f.states = append(f.states[:i], f.states[i+1:]...)
			slog.Info("path removed", "plugin", f.sourceType, "path", path)
			return
		}
	}
	slog.Warn("path not tracked, ignoring remove", "plugin", f.sourceType, "path", path)
}

func (f *FileSource) Poll() ([]core.Event, error) {
	f.mu.Lock()
	states := make([]*fileState, len(f.states))
	copy(states, f.states)
	f.mu.Unlock()

	var events []core.Event

	for _, st := range states {
		fi, err := os.Stat(st.path)
		if err != nil {
			continue
		}
		if fi.Size() < st.offset {
			slog.Warn("file truncated or rotated, resetting offset", "plugin", f.sourceType, "path", st.path)
			st.offset = 0
		}

		lines, newOffset, err := readNewLines(st.path, st.offset)
		if err != nil {
			slog.Error("failed to read lines", "plugin", f.sourceType, "path", st.path, "error", err)
			continue
		}

		for _, line := range lines {
			event := f.parse(line)
			if f.classifier != nil {
				event.Severity, event.Type = f.classifier.Classify(f.sourceType, event.Message)
			}
			events = append(events, event)
		}
		st.offset = newOffset

		if f.store != nil {
			_ = f.store.SetOffset(f.sourceType, st.path, st.offset)
		}
	}

	return events, nil
}

func readNewLines(path string, offset int64) ([]string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, offset, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			slog.Error("failed to close file", "error", err)
		}
	}(file)

	if _, err := file.Seek(offset, 0); err != nil {
		return nil, offset, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		slog.Error("scanner error, some lines may have been dropped", "path", path, "error", err)
	}

	newOffset, err := file.Seek(0, 1)
	if err != nil {
		return lines, offset, err
	}
	return lines, newOffset, nil
}

// Paths returns the file paths currently being tailed. Used by the worker's
// reload logic to diff against what's in the database and reconcile.
func (f *FileSource) Paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.states))
	for i, st := range f.states {
		out[i] = st.path
	}
	return out
}
