package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/metrics"
	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/storage"
)

type noopClient struct{}

func (noopClient) Forward(ctx context.Context, node config.Node, method string, key string, value string, consistency cluster.ConsistencyLevel) (cluster.RemoteResponse, error) {
	return cluster.RemoteResponse{}, nil
}

func (noopClient) Replicate(ctx context.Context, node config.Node, op cluster.Mutation) error {
	return nil
}

func (noopClient) Health(ctx context.Context, node config.Node) error {
	return nil
}

type noopLog struct{}

func (noopLog) Append(op persistence.Operation) (uint64, error) {
	return 1, nil
}

func TestHTTPKeyValueOperations(t *testing.T) {
	nodes := []config.Node{{ID: "node1", Address: "127.0.0.1:8081"}}
	store := storage.NewMemoryStore()
	registry := metrics.NewRegistry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	service := cluster.NewService("node1", nodes, cluster.NewHashRing(8, nodes), store, noopLog{}, noopClient{}, nil, registry, logger)
	server := NewServer(service, registry, logger)

	putBody := bytes.NewBufferString(`{"value":"Grace"}`)
	putReq := httptest.NewRequest(http.MethodPut, "/kv/user:2", putBody)
	putResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(putResp, putReq)
	if putResp.Code != http.StatusOK {
		t.Fatalf("expected PUT 200, got %d body=%s", putResp.Code, putResp.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/kv/user:2", nil)
	getResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("expected GET 200, got %d body=%s", getResp.Code, getResp.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(getResp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid get JSON: %v", err)
	}
	if payload["value"] != "Grace" {
		t.Fatalf("expected Grace, got %q", payload["value"])
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/kv/user:2", nil)
	deleteResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(deleteResp, deleteReq)
	if deleteResp.Code != http.StatusNoContent {
		t.Fatalf("expected DELETE 204, got %d", deleteResp.Code)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/kv/user:2", nil)
	missingResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingResp, missingReq)
	if missingResp.Code != http.StatusNotFound {
		t.Fatalf("expected GET missing 404, got %d", missingResp.Code)
	}
}
