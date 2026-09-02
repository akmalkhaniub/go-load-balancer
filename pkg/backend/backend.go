package backend

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akmalkhaniub/go-load-balancer/pkg/circuit"
)

// Backend represents an upstream server instance.
type Backend struct {
	URL          *url.URL
	Alive        bool
	Draining     bool
	Breaker      *circuit.Breaker
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
	activeConns  int64
	totalServed  uint64
}

// NewBackend creates and initializes a new upstream backend.
func NewBackend(rawURL string) (*Backend, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL %q: %w", rawURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(parsedURL)

	// Custom transport with aggressive connection pooling and timeouts
	proxy.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}

	b := &Backend{
		URL:          parsedURL,
		Alive:        true,
		Draining:     false,
		Breaker:      circuit.NewBreaker(3, 5*time.Second),
		ReverseProxy: proxy,
	}

	// Custom response modifier to record circuit breaker successes
	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.StatusCode >= 500 {
			b.Breaker.RecordFailure()
		} else {
			b.Breaker.RecordSuccess()
		}
		return nil
	}

	// Custom error handler when upstream fails
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		b.Breaker.RecordFailure()
		b.SetAlive(false)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, `{"error":"Upstream backend %s unreachable","details":"%v"}`+"\n", parsedURL.Host, e)
	}

	return b, nil
}

// SetAlive thread-safely updates the alive status of the backend.
func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Alive = alive
}

// IsAlive thread-safely checks if the backend is marked healthy and not circuit-broken.
func (b *Backend) IsAlive() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.Alive && !b.Draining && b.Breaker.Allow()
}

// SetDraining marks the backend as draining so existing connections finish without new ones arriving.
func (b *Backend) SetDraining(draining bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Draining = draining
}

// IsDraining returns true if backend is currently draining.
func (b *Backend) IsDraining() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.Draining
}

// IncrActive increases the in-flight connection counter.
func (b *Backend) IncrActive() {
	atomic.AddInt64(&b.activeConns, 1)
	atomic.AddUint64(&b.totalServed, 1)
}

// DecrActive decreases the in-flight connection counter.
func (b *Backend) DecrActive() {
	atomic.AddInt64(&b.activeConns, -1)
}

// GetActiveConns returns the current number of in-flight connections.
func (b *Backend) GetActiveConns() int64 {
	return atomic.LoadInt64(&b.activeConns)
}

// GetTotalServed returns the cumulative requests handled by this backend.
func (b *Backend) GetTotalServed() uint64 {
	return atomic.LoadUint64(&b.totalServed)
}

// CheckHealth performs a TCP dial check to verify if the upstream server is reachable.
func (b *Backend) CheckHealth(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", b.URL.Host)
	if err != nil {
		b.SetAlive(false)
		b.Breaker.RecordFailure()
		return false
	}
	_ = conn.Close()
	b.SetAlive(true)
	b.Breaker.RecordSuccess()
	return true
}
