package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/metrics"
	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/storage"
)

type ConsistencyLevel string

const (
	ConsistencyOne    ConsistencyLevel = "one"
	ConsistencyQuorum ConsistencyLevel = "quorum"
	ConsistencyAll    ConsistencyLevel = "all"
)

type Mutation struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type DurableLog interface {
	Append(op persistence.Operation) (uint64, error)
}

type Service struct {
	selfID            string
	nodes             map[string]config.Node
	ring              *HashRing
	store             storage.Store
	wal               DurableLog
	client            RemoteClient
	health            *HealthTracker
	metrics           *metrics.Registry
	logger            *slog.Logger
	replicationFactor int
	consistency       ConsistencyLevel
}

func NewService(selfID string, nodes []config.Node, ring *HashRing, store storage.Store, wal DurableLog, client RemoteClient, health *HealthTracker, registry *metrics.Registry, logger *slog.Logger) *Service {
	nodeMap := make(map[string]config.Node, len(nodes))
	for _, node := range nodes {
		nodeMap[node.ID] = node
	}
	return &Service{
		selfID:            selfID,
		nodes:             nodeMap,
		ring:              ring,
		store:             store,
		wal:               wal,
		client:            client,
		health:            health,
		metrics:           registry,
		logger:            logger,
		replicationFactor: len(nodes),
		consistency:       ConsistencyOne,
	}
}

func (s *Service) SetReplicationFactor(factor int) {
	if factor > 0 && factor <= len(s.nodes) {
		s.replicationFactor = factor
	}
}

func (s *Service) SetConsistency(consistency ConsistencyLevel) {
	switch consistency {
	case ConsistencyOne, ConsistencyQuorum, ConsistencyAll:
		s.consistency = consistency
	}
}

func (s *Service) Get(ctx context.Context, key string) (string, bool, error) {
	leader, ok := s.LeaderFor(key)
	if !ok {
		return "", false, errors.New("no leader available")
	}
	if leader.ID != s.selfID {
		s.metrics.IncForwardedRequest()
		resp, err := s.client.Forward(ctx, leader, http.MethodGet, key, "", s.consistency)
		if err != nil {
			s.metrics.IncFailure()
			return "", false, err
		}
		if resp.Status == http.StatusNotFound {
			return "", false, nil
		}
		if resp.Status < 200 || resp.Status >= 300 {
			return "", false, fmt.Errorf("leader returned status %d", resp.Status)
		}
		var payload struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(resp.Body, &payload); err != nil {
			return "", false, err
		}
		return payload.Value, true, nil
	}
	value, ok := s.store.Get(key)
	return value, ok, nil
}

func (s *Service) Put(ctx context.Context, key string, value string, consistency ConsistencyLevel) error {
	return s.mutate(ctx, Mutation{Op: http.MethodPut, Key: key, Value: value}, s.resolveConsistency(consistency))
}

func (s *Service) Delete(ctx context.Context, key string, consistency ConsistencyLevel) error {
	return s.mutate(ctx, Mutation{Op: http.MethodDelete, Key: key}, s.resolveConsistency(consistency))
}

func (s *Service) ApplyReplica(op Mutation) error {
	if err := s.appendWAL(op); err != nil {
		s.metrics.IncFailure()
		return err
	}
	if err := s.apply(op); err != nil {
		return err
	}
	s.metrics.IncReplication()
	return nil
}

func (s *Service) apply(op Mutation) error {
	switch op.Op {
	case http.MethodPut:
		s.store.Put(op.Key, op.Value)
	case http.MethodDelete:
		s.store.Delete(op.Key)
	default:
		return fmt.Errorf("unsupported replication op %q", op.Op)
	}
	return nil
}

func (s *Service) LeaderFor(key string) (config.Node, bool) {
	replicas := s.ReplicasFor(key)
	for _, replica := range replicas {
		if s.isHealthy(replica.ID) {
			return replica, true
		}
	}
	return config.Node{}, false
}

func (s *Service) ReplicasFor(key string) []config.Node {
	return s.ring.GetReplicas(key, s.replicationFactor)
}

func (s *Service) ClusterNodes() []NodeHealth {
	if s.health == nil {
		nodes := make([]NodeHealth, 0, len(s.nodes))
		for _, node := range s.nodes {
			nodes = append(nodes, NodeHealth{ID: node.ID, Address: node.Address, Healthy: true})
		}
		return nodes
	}
	return s.health.List()
}

func (s *Service) mutate(ctx context.Context, op Mutation, consistency ConsistencyLevel) error {
	leader, ok := s.LeaderFor(op.Key)
	if !ok {
		return errors.New("no leader available")
	}
	if leader.ID != s.selfID {
		s.metrics.IncForwardedRequest()
		resp, err := s.client.Forward(ctx, leader, op.Op, op.Key, op.Value, consistency)
		if err != nil {
			s.metrics.IncFailure()
			return err
		}
		if resp.Status < 200 || resp.Status >= 300 {
			return fmt.Errorf("leader returned status %d: %s", resp.Status, string(resp.Body))
		}
		return nil
	}

	if err := s.appendWAL(op); err != nil {
		s.metrics.IncFailure()
		return err
	}
	if err := s.apply(op); err != nil {
		return err
	}
	acks := 1 + s.replicateToFollowers(ctx, op)
	required := s.requiredAcks(op.Key, consistency)
	if acks < required {
		s.metrics.IncQuorumFailure()
		return fmt.Errorf("write consistency %q not satisfied: got %d ack(s), need %d", consistency, acks, required)
	}
	return nil
}

func (s *Service) replicateToFollowers(ctx context.Context, op Mutation) int {
	replicas := s.ReplicasFor(op.Key)
	var wg sync.WaitGroup
	acks := make(chan struct{}, len(replicas))
	for _, replica := range replicas {
		if replica.ID == s.selfID {
			continue
		}
		if !s.isHealthy(replica.ID) {
			s.logger.Warn("skipping unhealthy replica", "target_node", replica.ID, "key", op.Key)
			continue
		}
		replica := replica
		wg.Add(1)
		go func() {
			defer wg.Done()
			replicationCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if err := s.client.Replicate(replicationCtx, replica, op); err != nil {
				s.metrics.IncFailure()
				s.metrics.IncReplicationFailure()
				s.logger.Warn("replication failed", "target_node", replica.ID, "key", op.Key, "op", op.Op, "error", err)
				return
			}
			s.metrics.IncReplication()
			acks <- struct{}{}
		}()
	}
	wg.Wait()
	close(acks)
	return len(acks)
}

func (s *Service) appendWAL(op Mutation) error {
	if s.wal == nil {
		return nil
	}
	_, err := s.wal.Append(persistence.Operation{Op: op.Op, Key: op.Key, Value: op.Value})
	if err == nil {
		s.metrics.IncWALAppend()
	}
	return err
}

func (s *Service) requiredAcks(key string, consistency ConsistencyLevel) int {
	replicaCount := len(s.ReplicasFor(key))
	switch consistency {
	case ConsistencyAll:
		return replicaCount
	case ConsistencyQuorum:
		return replicaCount/2 + 1
	default:
		return 1
	}
}

func (s *Service) resolveConsistency(consistency ConsistencyLevel) ConsistencyLevel {
	if consistency == "" {
		return s.consistency
	}
	switch consistency {
	case ConsistencyOne, ConsistencyQuorum, ConsistencyAll:
		return consistency
	default:
		return s.consistency
	}
}

func (s *Service) isHealthy(nodeID string) bool {
	if s.health == nil {
		return true
	}
	return s.health.IsHealthy(nodeID)
}
