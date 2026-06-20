package persistence

import (
	"net/http"
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/storage"
)

func TestWALAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	seq1, err := wal.Append(Operation{Op: http.MethodPut, Key: "a", Value: "1"})
	if err != nil {
		t.Fatalf("append put: %v", err)
	}
	seq2, err := wal.Append(Operation{Op: http.MethodDelete, Key: "a"})
	if err != nil {
		t.Fatalf("append delete: %v", err)
	}
	if seq1 != 1 || seq2 != 2 {
		t.Fatalf("expected seqs 1 and 2, got %d and %d", seq1, seq2)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	var entries []WALEntry
	last, err := ReplayWAL(path, 0, func(entry WALEntry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if last != 2 || len(entries) != 2 {
		t.Fatalf("expected last seq 2 and 2 entries, got seq=%d len=%d", last, len(entries))
	}
}

func TestWALRejectsInvalidOperationWithoutAdvancingSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer wal.Close()

	if _, err := wal.Append(Operation{Op: "PATCH", Key: "a"}); err == nil {
		t.Fatal("expected invalid operation error")
	}
	seq, err := wal.Append(Operation{Op: http.MethodPut, Key: "a", Value: "1"})
	if err != nil {
		t.Fatalf("append after invalid op: %v", err)
	}
	if seq != 1 {
		t.Fatalf("expected sequence 1 after rejected op, got %d", seq)
	}
}

func TestRecoveryFromWALAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if _, err := wal.Append(Operation{Op: http.MethodPut, Key: "name", Value: "Ada"}); err != nil {
		t.Fatalf("append put: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	store := storage.NewMemoryStore()
	if _, err := ReplayWAL(path, 0, func(entry WALEntry) error {
		applyToStore(store, entry.Operation)
		return nil
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}

	value, ok := store.Get("name")
	if !ok || value != "Ada" {
		t.Fatalf("expected recovered value Ada, got %q ok=%v", value, ok)
	}
}

func applyToStore(store *storage.MemoryStore, op Operation) {
	switch op.Op {
	case http.MethodPut:
		store.Put(op.Key, op.Value)
	case http.MethodDelete:
		store.Delete(op.Key)
	}
}
