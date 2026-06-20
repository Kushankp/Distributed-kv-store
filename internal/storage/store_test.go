package storage

import (
	"strconv"
	"sync"
	"testing"
)

func TestMemoryStoreCRUD(t *testing.T) {
	store := NewMemoryStore()

	if _, ok := store.Get("missing"); ok {
		t.Fatal("expected missing key")
	}

	store.Put("name", "Ada")
	value, ok := store.Get("name")
	if !ok || value != "Ada" {
		t.Fatalf("expected Ada, got %q ok=%v", value, ok)
	}

	if deleted := store.Delete("name"); !deleted {
		t.Fatal("expected delete to report true")
	}
	if _, ok := store.Get("name"); ok {
		t.Fatal("expected deleted key to be absent")
	}
}

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	store := NewMemoryStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "key-" + strconv.Itoa(i)
			store.Put(key, strconv.Itoa(i))
			if _, ok := store.Get(key); !ok {
				t.Errorf("expected key %q", key)
			}
		}()
	}

	wg.Wait()
	if store.Len() != 100 {
		t.Fatalf("expected 100 keys, got %d", store.Len())
	}
}
