package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/metrics"
)

type Server struct {
	service *cluster.Service
	metrics *metrics.Registry
	logger  *slog.Logger
	mux     *http.ServeMux
}

func NewServer(service *cluster.Service, registry *metrics.Registry, logger *slog.Logger) *Server {
	server := &Server{
		service: service,
		metrics: registry,
		logger:  logger,
		mux:     http.NewServeMux(),
	}
	server.routes()
	return server
}

func (s *Server) ListenAndServe(address string) error {
	return http.ListenAndServe(address, loggingMiddleware(s.logger, s.mux))
}

func (s *Server) Handler() http.Handler {
	return loggingMiddleware(s.logger, s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/kv/", s.handleKV)
	s.mux.HandleFunc("/cluster/nodes", s.handleClusterNodes)
	s.mux.HandleFunc("/internal/replicate", s.handleReplicate)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	s.metrics.IncRequest(r.Method)

	switch r.Method {
	case http.MethodGet:
		value, ok, err := s.service.Get(r.Context(), key)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if !ok {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
	case http.MethodPut:
		var payload struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if err := s.service.Put(r.Context(), key, payload.Value, parseConsistency(r)); err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		if err := s.service.Delete(r.Context(), key, parseConsistency(r)); err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleClusterNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": s.service.ClusterNodes()})
}

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var op cluster.Mutation
	if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := s.service.ApplyReplica(op); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	data, err := s.metrics.JSON()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func parseConsistency(r *http.Request) cluster.ConsistencyLevel {
	return cluster.ConsistencyLevel(r.URL.Query().Get("consistency"))
}

func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Info("request", "method", r.Method, "path", r.URL.Path, "remote_addr", r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}
