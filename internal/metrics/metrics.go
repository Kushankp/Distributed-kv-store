package metrics

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

type Registry struct {
	requests            atomic.Uint64
	replications        atomic.Uint64
	replicationFailures atomic.Uint64
	walAppends          atomic.Uint64
	snapshots           atomic.Uint64
	failures            atomic.Uint64
	forwardedRequests   atomic.Uint64
	quorumFailures      atomic.Uint64
	mu                  sync.RWMutex
	perMethod           map[string]uint64
	nodeHealth          map[string]string
}

func NewRegistry() *Registry {
	return &Registry{perMethod: map[string]uint64{}, nodeHealth: map[string]string{}}
}

func (r *Registry) IncRequest(method string) {
	r.requests.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.perMethod[method]++
}

func (r *Registry) IncReplication() {
	r.replications.Add(1)
}

func (r *Registry) IncReplicationFailure() {
	r.replicationFailures.Add(1)
}

func (r *Registry) IncWALAppend() {
	r.walAppends.Add(1)
}

func (r *Registry) IncSnapshot() {
	r.snapshots.Add(1)
}

func (r *Registry) IncFailure() {
	r.failures.Add(1)
}

func (r *Registry) IncForwardedRequest() {
	r.forwardedRequests.Add(1)
}

func (r *Registry) IncQuorumFailure() {
	r.quorumFailures.Add(1)
}

func (r *Registry) SetNodeHealth(nodeID string, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeHealth[nodeID] = status
}

func (r *Registry) Snapshot() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	methods := make(map[string]uint64, len(r.perMethod))
	for method, count := range r.perMethod {
		methods[method] = count
	}
	nodeHealth := make(map[string]string, len(r.nodeHealth))
	for nodeID, status := range r.nodeHealth {
		nodeHealth[nodeID] = status
	}
	return map[string]any{
		"requests_total":             r.requests.Load(),
		"replications_total":         r.replications.Load(),
		"replication_failures_total": r.replicationFailures.Load(),
		"wal_appends_total":          r.walAppends.Load(),
		"snapshots_total":            r.snapshots.Load(),
		"failures_total":             r.failures.Load(),
		"forwarded_requests_total":   r.forwardedRequests.Load(),
		"quorum_failures_total":      r.quorumFailures.Load(),
		"requests_by_method":         methods,
		"node_health":                nodeHealth,
	}
}

func (r *Registry) JSON() ([]byte, error) {
	return json.MarshalIndent(r.Snapshot(), "", "  ")
}
