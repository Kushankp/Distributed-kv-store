package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/metrics"
	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/storage"
)

type fakeClient struct {
	mu            sync.Mutex
	forwards      []forwardCall
	replications  []replicationCall
	forwardResp   RemoteResponse
	forwardErr    error
	replicateErr  error
	replicateErrs map[string]error
	unhealthy     map[string]bool
}

type forwardCall struct {
	node   config.Node
	method string
	key    string
	value  string
}

type replicationCall struct {
	node config.Node
	op   Mutation
}

func (f *fakeClient) Forward(ctx context.Context, node config.Node, method string, key string, value string, consistency ConsistencyLevel) (RemoteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forwards = append(f.forwards, forwardCall{node: node, method: method, key: key, value: value})
	return f.forwardResp, f.forwardErr
}

func (f *fakeClient) Replicate(ctx context.Context, node config.Node, op Mutation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replications = append(f.replications, replicationCall{node: node, op: op})
	if f.replicateErrs != nil && f.replicateErrs[node.ID] != nil {
		return f.replicateErrs[node.ID]
	}
	return f.replicateErr
}

func (f *fakeClient) Health(ctx context.Context, node config.Node) error {
	if f.unhealthy != nil && f.unhealthy[node.ID] {
		return errors.New("unhealthy")
	}
	return nil
}

type noopLog struct{}

func (noopLog) Append(op persistence.Operation) (uint64, error) {
	return 1, nil
}

func TestServiceReplicatesLeaderPutToFollowers(t *testing.T) {
	nodes := testNodes()
	ring := NewHashRing(32, nodes)
	key := "alpha"
	leader, ok := ring.Get(key)
	if !ok {
		t.Fatal("expected leader")
	}

	store := storage.NewMemoryStore()
	client := &fakeClient{}
	service := newTestService(leader.ID, nodes, ring, store, client)

	if err := service.Put(context.Background(), key, "value-1", ConsistencyOne); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	value, ok := store.Get(key)
	if !ok || value != "value-1" {
		t.Fatalf("expected local leader value, got %q ok=%v", value, ok)
	}
	if len(client.replications) != len(nodes)-1 {
		t.Fatalf("expected %d replications, got %d", len(nodes)-1, len(client.replications))
	}
	for _, call := range client.replications {
		if call.node.ID == leader.ID {
			t.Fatalf("should not replicate back to leader %q", leader.ID)
		}
		if call.op.Op != http.MethodPut || call.op.Key != key || call.op.Value != "value-1" {
			t.Fatalf("unexpected replication op: %+v", call.op)
		}
	}
}

func TestServiceForwardsWriteToLeader(t *testing.T) {
	nodes := testNodes()
	ring := NewHashRing(32, nodes)
	key := "bravo"
	leader, ok := ring.Get(key)
	if !ok {
		t.Fatal("expected leader")
	}

	var follower config.Node
	for _, node := range nodes {
		if node.ID != leader.ID {
			follower = node
			break
		}
	}

	client := &fakeClient{forwardResp: RemoteResponse{Status: http.StatusOK}}
	service := newTestService(follower.ID, nodes, ring, storage.NewMemoryStore(), client)

	if err := service.Put(context.Background(), key, "remote-value", ConsistencyOne); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if len(client.forwards) != 1 {
		t.Fatalf("expected 1 forward, got %d", len(client.forwards))
	}
	call := client.forwards[0]
	if call.node.ID != leader.ID || call.method != http.MethodPut || call.key != key || call.value != "remote-value" {
		t.Fatalf("unexpected forward call: %+v", call)
	}
}

func TestServiceForwardsGetAndDecodesLeaderResponse(t *testing.T) {
	nodes := testNodes()
	ring := NewHashRing(32, nodes)
	key := "charlie"
	leader, ok := ring.Get(key)
	if !ok {
		t.Fatal("expected leader")
	}

	var follower config.Node
	for _, node := range nodes {
		if node.ID != leader.ID {
			follower = node
			break
		}
	}

	body, _ := json.Marshal(map[string]string{"value": "from-leader"})
	client := &fakeClient{forwardResp: RemoteResponse{Status: http.StatusOK, Body: body}}
	service := newTestService(follower.ID, nodes, ring, storage.NewMemoryStore(), client)

	value, found, err := service.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !found || value != "from-leader" {
		t.Fatalf("expected forwarded value, got %q found=%v", value, found)
	}
}

func TestServiceAppliesReplicaMutation(t *testing.T) {
	nodes := testNodes()
	store := storage.NewMemoryStore()
	service := newTestService(nodes[0].ID, nodes, NewHashRing(16, nodes), store, &fakeClient{})

	if err := service.ApplyReplica(Mutation{Op: http.MethodPut, Key: "delta", Value: "replicated"}); err != nil {
		t.Fatalf("apply replica failed: %v", err)
	}
	value, ok := store.Get("delta")
	if !ok || value != "replicated" {
		t.Fatalf("expected replicated value, got %q ok=%v", value, ok)
	}

	if err := service.ApplyReplica(Mutation{Op: http.MethodDelete, Key: "delta"}); err != nil {
		t.Fatalf("delete replica failed: %v", err)
	}
	if _, ok := store.Get("delta"); ok {
		t.Fatal("expected key deleted by replica mutation")
	}
}

func TestServiceQuorumWriteSuccess(t *testing.T) {
	nodes := testNodes()
	ring := NewHashRing(32, nodes)
	key := "quorum-success"
	leader, _ := ring.Get(key)
	client := &fakeClient{replicateErrs: map[string]error{}}
	service := newTestService(leader.ID, nodes, ring, storage.NewMemoryStore(), client)

	if err := service.Put(context.Background(), key, "ok", ConsistencyQuorum); err != nil {
		t.Fatalf("expected quorum success, got %v", err)
	}
}

func TestServiceQuorumWriteFailure(t *testing.T) {
	nodes := testNodes()
	ring := NewHashRing(32, nodes)
	key := "quorum-failure"
	leader, _ := ring.Get(key)
	errs := map[string]error{}
	for _, node := range nodes {
		if node.ID != leader.ID {
			errs[node.ID] = errors.New("replication down")
		}
	}
	client := &fakeClient{replicateErrs: errs}
	service := newTestService(leader.ID, nodes, ring, storage.NewMemoryStore(), client)

	if err := service.Put(context.Background(), key, "fail", ConsistencyQuorum); err == nil {
		t.Fatal("expected quorum failure")
	}
}

func TestServiceSkipsUnhealthyReplica(t *testing.T) {
	nodes := testNodes()
	ring := NewHashRing(32, nodes)
	key := "health-skip"
	leader, _ := ring.Get(key)
	unhealthy := map[string]bool{}
	var unhealthyID string
	for _, node := range nodes {
		if node.ID != leader.ID {
			unhealthy[node.ID] = true
			unhealthyID = node.ID
			break
		}
	}
	client := &fakeClient{unhealthy: unhealthy}
	registry := metrics.NewRegistry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	health := NewHealthTracker(leader.ID, nodes, client, registry, logger)
	health.CheckOnce(context.Background())
	service := NewService(leader.ID, nodes, ring, storage.NewMemoryStore(), noopLog{}, client, health, registry, logger)
	service.SetReplicationFactor(len(nodes))

	if err := service.Put(context.Background(), key, "ok", ConsistencyOne); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	for _, call := range client.replications {
		if call.node.ID == unhealthyID {
			t.Fatalf("replicated to unhealthy node %q", unhealthyID)
		}
	}
}

func newTestService(selfID string, nodes []config.Node, ring *HashRing, store storage.Store, client RemoteClient) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	service := NewService(selfID, nodes, ring, store, noopLog{}, client, nil, metrics.NewRegistry(), logger)
	service.SetReplicationFactor(len(nodes))
	return service
}
