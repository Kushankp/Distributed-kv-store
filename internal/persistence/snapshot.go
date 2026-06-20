package persistence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type Snapshot struct {
	Seq       uint64            `json:"seq"`
	CreatedAt time.Time         `json:"created_at"`
	Data      map[string]string `json:"data"`
}

func SaveSnapshot(path string, seq uint64, data map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	snap := Snapshot{Seq: seq, CreatedAt: time.Now().UTC(), Data: data}
	payload, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func LoadSnapshot(path string) (Snapshot, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{Data: map[string]string{}}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, err
	}
	if snap.Data == nil {
		snap.Data = map[string]string{}
	}
	return snap, nil
}
