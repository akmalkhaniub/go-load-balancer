package api

import (
	"encoding/json"
	"net/http"

	"github.com/akmalkhaniub/go-load-balancer/pkg/backend"
	"github.com/akmalkhaniub/go-load-balancer/pkg/balancer"
)

type AdminServer struct {
	pool *balancer.ServerPool
	mux  *http.ServeMux
}

func NewAdminServer(pool *balancer.ServerPool) *AdminServer {
	s := &AdminServer{
		pool: pool,
		mux:  http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

type BackendRequest struct {
	URL string `json:"url"`
}

type BackendDetail struct {
	URL          string `json:"url"`
	Alive        bool   `json:"alive"`
	Draining     bool   `json:"draining"`
	BreakerState string `json:"breaker_state"`
	ActiveConns  int64  `json:"active_connections"`
	TotalServed  uint64 `json:"total_served"`
}

func (s *AdminServer) registerRoutes() {
	s.mux.HandleFunc("GET /api/backends", s.handleGetBackends)
	s.mux.HandleFunc("POST /api/backends", s.handleAddBackend)
	s.mux.HandleFunc("DELETE /api/backends", s.handleRemoveBackend)
	s.mux.HandleFunc("POST /api/backends/drain", s.handleDrainBackend)
}

func (s *AdminServer) handleGetBackends(w http.ResponseWriter, r *http.Request) {
	backends := s.pool.GetBackends()
	details := make([]BackendDetail, 0, len(backends))

	for _, b := range backends {
		details = append(details, BackendDetail{
			URL:          b.URL.String(),
			Alive:        b.IsAlive(),
			Draining:     b.IsDraining(),
			BreakerState: string(b.Breaker.GetState()),
			ActiveConns:  b.GetActiveConns(),
			TotalServed:  b.GetTotalServed(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(details)
}

func (s *AdminServer) handleAddBackend(w http.ResponseWriter, r *http.Request) {
	var req BackendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, `{"error":"valid JSON body with 'url' field is required"}`, http.StatusBadRequest)
		return
	}

	b, err := backend.NewBackend(req.URL)
	if err != nil {
		http.Error(w, `{"error":"invalid backend URL format"}`, http.StatusBadRequest)
		return
	}

	s.pool.AddBackend(b)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": "Backend registered dynamically",
		"url":     b.URL.String(),
	})
}

func (s *AdminServer) handleRemoveBackend(w http.ResponseWriter, r *http.Request) {
	var req BackendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, `{"error":"valid JSON body with 'url' field is required"}`, http.StatusBadRequest)
		return
	}

	if err := s.pool.RemoveBackend(req.URL); err != nil {
		http.Error(w, `{"error":"backend not found in pool"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": "Backend deregistered successfully",
		"url":     req.URL,
	})
}

func (s *AdminServer) handleDrainBackend(w http.ResponseWriter, r *http.Request) {
	var req BackendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, `{"error":"valid JSON body with 'url' field is required"}`, http.StatusBadRequest)
		return
	}

	if err := s.pool.DrainBackend(req.URL); err != nil {
		http.Error(w, `{"error":"backend not found in pool"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": "Backend marked as draining",
		"url":     req.URL,
	})
}

func (s *AdminServer) Handler() http.Handler {
	return s.mux
}
