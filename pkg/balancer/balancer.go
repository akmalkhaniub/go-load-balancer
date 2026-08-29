package balancer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akmalkhaniub/go-load-balancer/pkg/backend"
)

// Algorithm defines the load balancing strategy.
type Algorithm string

const (
	AlgorithmRoundRobin       Algorithm = "round-robin"
	AlgorithmLeastConnections Algorithm = "least-connections"
	AlgorithmIPHash           Algorithm = "ip-hash"
)

// ServerPool holds the collection of registered upstream backends.
type ServerPool struct {
	backends  []*backend.Backend
	current   uint64
	mux       sync.RWMutex
	algorithm Algorithm
}

// NewServerPool initializes an empty server pool with the given algorithm.
func NewServerPool(algo Algorithm) *ServerPool {
	return &ServerPool{
		backends:  make([]*backend.Backend, 0),
		algorithm: algo,
	}
}

// AddBackend registers a backend with the pool.
func (s *ServerPool) AddBackend(b *backend.Backend) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.backends = append(s.backends, b)
}

// GetBackends returns a snapshot of all registered backends.
func (s *ServerPool) GetBackends() []*backend.Backend {
	s.mux.RLock()
	defer s.mux.RUnlock()
	dst := make([]*backend.Backend, len(s.backends))
	copy(dst, s.backends)
	return dst
}

// NextIndex atomically advances and returns the next index for round-robin.
func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

// GetNextBackend selects the next available backend using the configured strategy.
func (s *ServerPool) GetNextBackend(clientIP string) (*backend.Backend, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	if len(s.backends) == 0 {
		return nil, errors.New("no backends registered in server pool")
	}

	switch s.algorithm {
	case AlgorithmLeastConnections:
		return s.getLeastConnectionBackend()
	case AlgorithmIPHash:
		return s.getIPHashBackend(clientIP)
	case AlgorithmRoundRobin:
		fallthrough
	default:
		return s.getRoundRobinBackend()
	}
}

func (s *ServerPool) getRoundRobinBackend() (*backend.Backend, error) {
	next := s.NextIndex()
	l := len(s.backends) + next

	for i := next; i < l; i++ {
		idx := i % len(s.backends)
		if s.backends[idx].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(idx))
			}
			return s.backends[idx], nil
		}
	}
	return nil, errors.New("all upstream backends are down")
}

func (s *ServerPool) getLeastConnectionBackend() (*backend.Backend, error) {
	var selected *backend.Backend
	var minConns int64 = -1

	for _, b := range s.backends {
		if !b.IsAlive() {
			continue
		}
		conns := b.GetActiveConns()
		if minConns == -1 || conns < minConns {
			minConns = conns
			selected = b
		}
	}

	if selected == nil {
		return nil, errors.New("all upstream backends are down")
	}
	return selected, nil
}

func (s *ServerPool) getIPHashBackend(clientIP string) (*backend.Backend, error) {
	h := fnv.New32a()
	h.Write([]byte(clientIP))
	hashVal := int(h.Sum32())

	// Find the first alive backend starting from hash offset
	for i := 0; i < len(s.backends); i++ {
		idx := (hashVal + i) % len(s.backends)
		if s.backends[idx].IsAlive() {
			return s.backends[idx], nil
		}
	}
	return nil, errors.New("all upstream backends are down")
}

// StartHealthCheck starts periodic background health verification in a dedicated goroutine.
func (s *ServerPool) StartHealthCheck(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				slog.Info("Health check worker stopped")
				return
			case <-ticker.C:
				s.mux.RLock()
				for _, b := range s.backends {
					status := "UP"
					alive := b.CheckHealth(2 * time.Second)
					if !alive {
						status = "DOWN"
					}
					slog.Debug("Backend health verified", "url", b.URL.String(), "status", status)
				}
				s.mux.RUnlock()
			}
		}
	}()
}

// ServeHTTP implements http.Handler to proxy incoming client requests.
func (s *ServerPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	clientIP := r.RemoteAddr
	target, err := s.GetNextBackend(clientIP)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusServiceUnavailable)
		return
	}

	target.IncrActive()
	defer target.DecrActive()

	// Forward request headers
	r.Header.Set("X-Forwarded-Host", r.Host)
	r.Header.Set("X-Forwarded-For", clientIP)
	r.Header.Set("X-Load-Balancer", "go-load-balancer/v1")

	target.ReverseProxy.ServeHTTP(w, r)
}
