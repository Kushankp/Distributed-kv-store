package storage

import "sync"

type Store interface {
	Get(key string) (string, bool)
	Put(key string, value string)
	Delete(key string) bool
	Len() int
	Snapshot() map[string]string
	Restore(data map[string]string)
}

type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: map[string]string{}}
}

func (s *MemoryStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

func (s *MemoryStore) Put(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *MemoryStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	delete(s.data, key)
	return true
}

func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *MemoryStore) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := make(map[string]string, len(s.data))
	for key, value := range s.data {
		data[key] = value
	}
	return data
}

func (s *MemoryStore) Restore(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string, len(data))
	for key, value := range data {
		s.data[key] = value
	}
}
