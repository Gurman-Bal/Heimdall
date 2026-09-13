package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"heimdall/internal/core"
)

func testParse(line string) core.Event {
	return core.Event{Source: "test", Message: line}
}

func TestFileSourceReadsOnlyNewLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	if err := os.WriteFile(path, []byte("old line 1\nold line 2\n"), 0644); err != nil {
		t.Fatal(err)
	}

	src := NewFileSource("test", []string{path}, testParse, nil, nil)
	if err := src.Start(); err != nil {
		t.Fatal(err)
	}

	// Start() should seed the offset at EOF - a first Poll should see nothing.
	events, err := src.Poll()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events on first poll (pre-existing content shouldn't replay), got %d", len(events))
	}

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("new line 1\n")
	f.Close()

	events, err = src.Poll()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Message != "new line 1" {
		t.Errorf("expected exactly the new line, got %+v", events)
	}
}

func TestFileSourceHandlesRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	os.WriteFile(path, []byte("aaaaaaaaaaaaaaaaaaaa\n"), 0644)

	src := NewFileSource("test", []string{path}, testParse, nil, nil)
	src.Start()
	src.Poll() // establish offset past the long line

	// simulate log rotation: file gets truncated and replaced with something shorter
	os.WriteFile(path, []byte("short\n"), 0644)

	events, err := src.Poll()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Message != "short" {
		t.Errorf("rotation not handled correctly, got %+v", events)
	}
}

func TestFileSourceAddRemovePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "added.log")
	os.WriteFile(path, []byte(""), 0644)

	src := NewFileSource("test", nil, testParse, nil, nil)
	src.Start()

	src.AddPath(path)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("hello\n")
	f.Close()

	events, _ := src.Poll()
	if len(events) != 1 {
		t.Fatalf("expected 1 event after AddPath, got %d", len(events))
	}

	src.RemovePath(path)
	f, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("should not appear\n")
	f.Close()

	events, _ = src.Poll()
	if len(events) != 0 {
		t.Errorf("expected 0 events after RemovePath, got %d", len(events))
	}
}
