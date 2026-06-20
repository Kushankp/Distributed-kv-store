package persistence

import (
	"net/http"
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/storage"
)

func TestSnapshotSaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")
	data := map[string]string{"a": "1", "b": "2"}

	if err := SaveSnapshot(path, 7, data); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	snap, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snap.Seq != 7 || snap.Data["a"] != "1" || snap.Data["b"] != "2" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestRecoveryFromSnapshotAndWAL(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")
	walPath := filepath.Join(dir, "wal.log")

	if err := SaveSnapshot(snapshotPath, 2, map[string]string{"a": "1", "old": "gone"}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	_, _ = wal.Append(Operation{Op: http.MethodPut, Key: "ignored", Value: "before-snapshot"})
	_, _ = wal.Append(Operation{Op: http.MethodPut, Key: "also-ignored", Value: "before-snapshot"})
	if _, err := wal.Append(Operation{Op: http.MethodPut, Key: "b", Value: "2"}); err != nil {
		t.Fatalf("append post-snapshot put: %v", err)
	}
	if _, err := wal.Append(Operation{Op: http.MethodDelete, Key: "old"}); err != nil {
		t.Fatalf("append post-snapshot delete: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	snap, err := LoadSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	store := storage.NewMemoryStore()
	store.Restore(snap.Data)
	if _, err := ReplayWAL(walPath, snap.Seq, func(entry WALEntry) error {
		applyToStore(store, entry.Operation)
		return nil
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}

	if value, ok := store.Get("a"); !ok || value != "1" {
		t.Fatalf("expected snapshot value a=1, got %q ok=%v", value, ok)
	}
	if value, ok := store.Get("b"); !ok || value != "2" {
		t.Fatalf("expected wal value b=2, got %q ok=%v", value, ok)
	}
	if _, ok := store.Get("old"); ok {
		t.Fatal("expected old key deleted by wal replay")
	}
	if _, ok := store.Get("ignored"); ok {
		t.Fatal("expected pre-snapshot wal entry to be skipped")
	}
}
