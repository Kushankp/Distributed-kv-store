package cluster

import (
	"testing"

	"distributed-kv-store/internal/config"
)

func testNodes() []config.Node {
	return []config.Node{
		{ID: "node1", Address: "127.0.0.1:8081"},
		{ID: "node2", Address: "127.0.0.1:8082"},
		{ID: "node3", Address: "127.0.0.1:8083"},
	}
}

func TestHashRingReturnsStableLeader(t *testing.T) {
	ring := NewHashRing(32, testNodes())

	first, ok := ring.Get("customer:123")
	if !ok {
		t.Fatal("expected leader")
	}
	for i := 0; i < 20; i++ {
		next, ok := ring.Get("customer:123")
		if !ok {
			t.Fatal("expected leader")
		}
		if next.ID != first.ID {
			t.Fatalf("expected stable leader %q, got %q", first.ID, next.ID)
		}
	}
}

func TestHashRingReturnsDistinctReplicas(t *testing.T) {
	ring := NewHashRing(32, testNodes())
	replicas := ring.GetReplicas("invoice:987", 3)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}

	seen := map[string]bool{}
	for _, replica := range replicas {
		if seen[replica.ID] {
			t.Fatalf("duplicate replica %q", replica.ID)
		}
		seen[replica.ID] = true
	}
}

func TestHashRingCapsReplicaCountAtNodeCount(t *testing.T) {
	ring := NewHashRing(4, testNodes())
	replicas := ring.GetReplicas("order:1", 20)
	if len(replicas) != len(testNodes()) {
		t.Fatalf("expected replicas capped at node count, got %d", len(replicas))
	}
}
