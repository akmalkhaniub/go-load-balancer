package balancer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"strings"
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

// RemoveBackend dynamically removes a backend from the pool by URL.
func (s *ServerPool) RemoveBackend(rawURL string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	idx := -1
	for i, b := range s.backends {
		if b.URL.String() == rawURL || strings.TrimRight(b.URL.String(), "/") == strings.TrimRight(rawURL, "/") {
			idx = i
			break
		}
	}

	if idx == -1 {
		return fmt.Errorf("backend %q not found in pool", rawURL)
	}

	s.backends = append(s.backends[:idx], s.backends[idx+1:]...)
	return nil
}

// DrainBackend sets a backend to draining mode so it finishes existing work without taking new requests.
func (s *ServerPool) DrainBackend(rawURL string) error {
	s.mux.RLock()
	defer s.mux.RUnlock()

	for _, b := range s.backends {
		if b.URL.String() == rawURL || strings.TrimRight(b.URL.String(), "/") == strings.TrimRight(rawURL, "/") {
			b.SetDraining(true)
			return nil
		}
	}
	return fmt.Errorf("backend %q not found", rawURL)
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
	s.mux.RLock()
	defer s.mux.RUnlock()
	if len(s.backends) == 0 {
		return 0
	}
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
	if len(s.backends) == 0 {
		return nil, errors.New("no backends registered in server pool")
	}
	next := int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
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
	return nil, errors.New("all upstream backends are down or circuit-broken")
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
		return nil, errors.New("all upstream backends are down or circuit-broken")
	}
	return selected, nil
}

func (s *ServerPool) getIPHashBackend(clientIP string) (*backend.Backend, error) {
	h := fnv.New32a()
	h.Write([]byte(clientIP))
	hashVal := int(h.Sum32())

	for i := 0; i < len(s.backends); i++ {
		idx := (hashVal + i) % len(s.backends)
		if s.backends[idx].IsAlive() {
			return s.backends[idx], nil
		}
	}
	return nil, errors.New("all upstream backends are down or circuit-broken")
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
					slog.Debug("Backend health verified",
						"url", b.URL.String(),
						"status", status,
						"breaker", b.Breaker.GetState(),
					)
				}
				s.mux.RUnlock()
			}
		}
	}()
}

// ServeHTTP implements http.Handler to proxy incoming client requests with WebSocket upgrade awareness.
func (s *ServerPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	clientIP := r.RemoteAddr
	target, err := s.GetNextBackend(clientIP)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"error":"%s"}`+"\n", err.Error())
		return
	}

	target.IncrActive()
	defer target.DecrActive()

	// Forward standard proxy and WebSocket headers
	r.Header.Set("X-Forwarded-Host", r.Host)
	r.Header.Set("X-Forwarded-For", clientIP)
	r.Header.Set("X-Load-Balancer", "go-load-balancer/v2")

	target.ReverseProxy.ServeHTTP(w, r)
}
