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
)

// Backend represents an upstream server instance.
type Backend struct {
	URL          *url.URL
	Alive        bool
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
		ReverseProxy: proxy,
	}

	// Custom error handler when upstream fails
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
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

// IsAlive thread-safely checks if the backend is marked healthy.
func (b *Backend) IsAlive() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.Alive
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
		return false
	}
	_ = conn.Close()
	b.SetAlive(true)
	return true
}
