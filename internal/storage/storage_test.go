package storage

import "testing"

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestListSourcesReturnsEmptySliceNotNil(t *testing.T) {
	store := newTestStore(t)

	sources, err := store.ListSources("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if sources == nil {
		t.Error("expected empty slice, got nil - this serializes as JSON null and breaks the frontend")
	}
	if len(sources) != 0 {
		t.Errorf("expected 0 sources, got %d", len(sources))
	}
}

func TestAddAndListSources(t *testing.T) {
	store := newTestStore(t)

	if _, err := store.AddSource("truenas", "/var/log/messages"); err != nil {
		t.Fatal(err)
	}

	sources, err := store.ListSources("truenas")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].Path != "/var/log/messages" {
		t.Errorf("got %+v", sources)
	}
}

func TestRemoveSource(t *testing.T) {
	store := newTestStore(t)

	id, _ := store.AddSource("truenas", "/var/log/messages")
	if err := store.RemoveSource(id); err != nil {
		t.Fatal(err)
	}

	sources, _ := store.ListSources("truenas")
	if len(sources) != 0 {
		t.Errorf("expected source to be gone after removal, got %+v", sources)
	}
}

func TestOffsetRoundTrip(t *testing.T) {
	store := newTestStore(t)

	if err := store.SetOffset("truenas", "/var/log/messages", 1024); err != nil {
		t.Fatal(err)
	}

	offset, found, err := store.GetOffset("truenas", "/var/log/messages")
	if err != nil {
		t.Fatal(err)
	}
	if !found || offset != 1024 {
		t.Errorf("got (offset=%d, found=%v), want (1024, true)", offset, found)
	}
}

func TestGetOffsetNotFound(t *testing.T) {
	store := newTestStore(t)

	_, found, err := store.GetOffset("truenas", "/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("expected found=false for an offset that was never set")
	}
}
