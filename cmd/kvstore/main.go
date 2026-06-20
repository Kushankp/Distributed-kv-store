package main

import (
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/httpapi"
	"distributed-kv-store/internal/metrics"
	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/storage"
)

func main() {
	configPath := flag.String("config", "configs/node1.json", "path to node configuration")
	snapshotInterval := flag.Duration("snapshot-interval", -1, "override snapshot interval, for example 10s; 0 disables snapshots")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if *snapshotInterval >= 0 {
		cfg.SnapshotInterval.Duration = *snapshotInterval
	}

	self, ok := cfg.Self()
	if !ok {
		logger.Error("self node not found in config", "node_id", cfg.NodeID)
		os.Exit(1)
	}

	store := storage.NewMemoryStore()
	registry := metrics.NewRegistry()
	ring := cluster.NewHashRing(cfg.Replication.VirtualNodes, cfg.Nodes)
	client := cluster.NewClient(cfg.RequestTimeout.Duration, cfg.RetryCount, logger)
	walPath := filepath.Join(cfg.DataDir, cfg.NodeID, "wal.log")
	snapshotPath := filepath.Join(cfg.DataDir, cfg.NodeID, "snapshot.json")
	wal, err := persistence.OpenWAL(walPath)
	if err != nil {
		logger.Error("failed to open wal", "error", err)
		os.Exit(1)
	}
	defer wal.Close()

	snap, err := persistence.LoadSnapshot(snapshotPath)
	if err != nil {
		logger.Error("failed to load snapshot", "error", err)
		os.Exit(1)
	}
	store.Restore(snap.Data)
	if _, err := persistence.ReplayWAL(walPath, snap.Seq, func(entry persistence.WALEntry) error {
		switch entry.Op {
		case "PUT":
			store.Put(entry.Key, entry.Value)
		case "DELETE":
			store.Delete(entry.Key)
		}
		return nil
	}); err != nil {
		logger.Error("failed to replay wal", "error", err)
		os.Exit(1)
	}

	health := cluster.NewHealthTracker(cfg.NodeID, cfg.Nodes, client, registry, logger)
	service := cluster.NewService(cfg.NodeID, cfg.Nodes, ring, store, wal, client, health, registry, logger)
	service.SetReplicationFactor(cfg.Replication.Factor)
	service.SetConsistency(cluster.ConsistencyLevel(cfg.Replication.Consistency))

	stopHeartbeat := health.Start(cfg.HeartbeatInterval.Duration)
	defer stopHeartbeat()
	if cfg.SnapshotInterval.Duration > 0 {
		go runSnapshots(cfg.SnapshotInterval.Duration, snapshotPath, wal, store, registry, logger)
	}

	server := httpapi.NewServer(service, registry, logger)
	logger.Info("starting kv node", "node_id", cfg.NodeID, "address", self.Address)
	if err := server.ListenAndServe(self.Address); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func runSnapshots(interval time.Duration, path string, wal *persistence.WAL, store storage.Store, registry *metrics.Registry, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := persistence.SaveSnapshot(path, wal.LastSeq(), store.Snapshot()); err != nil {
			registry.IncFailure()
			logger.Warn("snapshot failed", "path", path, "error", err)
			continue
		}
		registry.IncSnapshot()
		logger.Info("snapshot saved", "path", path, "seq", wal.LastSeq())
	}
}
