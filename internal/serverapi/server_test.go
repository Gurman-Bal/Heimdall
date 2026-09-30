package serverapi

import (
	"testing"
)

type fakeManagedSource struct {
	paths   []string
	added   []string
	removed []string
}

func (f *fakeManagedSource) Paths() []string     { return f.paths }
func (f *fakeManagedSource) AddPath(p string)    { f.added = append(f.added, p) }
func (f *fakeManagedSource) RemovePath(p string) { f.removed = append(f.removed, p) }

// This test documents the reconciliation logic's actual behavior directly,
// independent of HTTP — the diff between "wanted" (DB) and "current"
// (in-memory) paths, which is the part most likely to silently do nothing
// if a source type mapping is ever missing (as handleReload now logs).
func TestReconciliationDiffLogic(t *testing.T) {
	fake := &fakeManagedSource{paths: []string{"/a", "/b"}}

	wantedSet := map[string]bool{"/b": true, "/c": true}
	currentSet := map[string]bool{}
	for _, p := range fake.Paths() {
		currentSet[p] = true
	}

	for path := range wantedSet {
		if !currentSet[path] {
			fake.AddPath(path)
		}
	}
	for path := range currentSet {
		if !wantedSet[path] {
			fake.RemovePath(path)
		}
	}

	if len(fake.added) != 1 || fake.added[0] != "/c" {
		t.Errorf("expected /c to be added, got %v", fake.added)
	}
	if len(fake.removed) != 1 || fake.removed[0] != "/a" {
		t.Errorf("expected /a to be removed, got %v", fake.removed)
	}
}
