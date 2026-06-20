package persistence

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

type Operation struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type WALEntry struct {
	Seq uint64 `json:"seq"`
	Operation
}

type WAL struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	lastSeq uint64
}

func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	lastSeq, err := LastSequence(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, file: file, lastSeq: lastSeq}, nil
}

func (w *WAL) Append(op Operation) (uint64, error) {
	if op.Key == "" {
		return 0, errors.New("wal operation key is required")
	}
	if op.Op != http.MethodPut && op.Op != http.MethodDelete {
		return 0, fmt.Errorf("unsupported wal operation %q", op.Op)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{Seq: w.lastSeq + 1, Operation: op}
	data, err := json.Marshal(entry)
	if err != nil {
		return 0, err
	}
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return 0, err
	}
	if err := w.file.Sync(); err != nil {
		return 0, err
	}
	w.lastSeq = entry.Seq
	return entry.Seq, nil
}

func (w *WAL) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeq
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func ReplayWAL(path string, afterSeq uint64, apply func(WALEntry) error) (uint64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return afterSeq, nil
	}
	if err != nil {
		return afterSeq, err
	}
	defer file.Close()

	var lastSeq uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return lastSeq, err
		}
		lastSeq = entry.Seq
		if entry.Seq <= afterSeq {
			continue
		}
		if err := apply(entry); err != nil {
			return lastSeq, err
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return lastSeq, nil
		}
		return lastSeq, err
	}
	if lastSeq < afterSeq {
		return afterSeq, nil
	}
	return lastSeq, nil
}

func LastSequence(path string) (uint64, error) {
	var last uint64
	_, err := ReplayWAL(path, 0, func(entry WALEntry) error {
		last = entry.Seq
		return nil
	})
	return last, err
}
