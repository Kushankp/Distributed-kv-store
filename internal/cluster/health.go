package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/metrics"
)

type NodeHealth struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Healthy bool   `json:"healthy"`
}

type HealthTracker struct {
	selfID  string
	nodes   map[string]config.Node
	client  RemoteClient
	metrics *metrics.Registry
	logger  *slog.Logger
	mu      sync.RWMutex
	healthy map[string]bool
}

func NewHealthTracker(selfID string, nodes []config.Node, client RemoteClient, registry *metrics.Registry, logger *slog.Logger) *HealthTracker {
	nodeMap := make(map[string]config.Node, len(nodes))
	healthy := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		nodeMap[node.ID] = node
		healthy[node.ID] = true
		registry.SetNodeHealth(node.ID, "healthy")
	}
	return &HealthTracker{selfID: selfID, nodes: nodeMap, client: client, metrics: registry, logger: logger, healthy: healthy}
}

func (h *HealthTracker) Start(interval time.Duration) func() {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		h.CheckOnce(context.Background())
		for {
			select {
			case <-ticker.C:
				h.CheckOnce(context.Background())
			case <-stop:
				return
			}
		}
	}()
	return func() { close(stop) }
}

func (h *HealthTracker) CheckOnce(ctx context.Context) {
	for _, node := range h.nodes {
		if node.ID == h.selfID {
			h.Set(node.ID, true)
			continue
		}
		checkCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		err := h.client.Health(checkCtx, node)
		cancel()
		h.Set(node.ID, err == nil)
	}
}

func (h *HealthTracker) Set(nodeID string, healthy bool) {
	h.mu.Lock()
	previous := h.healthy[nodeID]
	h.healthy[nodeID] = healthy
	h.mu.Unlock()

	status := "unhealthy"
	if healthy {
		status = "healthy"
	}
	h.metrics.SetNodeHealth(nodeID, status)
	if previous != healthy {
		h.logger.Info("node health changed", "node_id", nodeID, "healthy", healthy)
	}
}

func (h *HealthTracker) IsHealthy(nodeID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	healthy, ok := h.healthy[nodeID]
	return !ok || healthy
}

func (h *HealthTracker) List() []NodeHealth {
	h.mu.RLock()
	defer h.mu.RUnlock()
	nodes := make([]NodeHealth, 0, len(h.nodes))
	for _, node := range h.nodes {
		nodes = append(nodes, NodeHealth{ID: node.ID, Address: node.Address, Healthy: h.healthy[node.ID]})
	}
	return nodes
}
