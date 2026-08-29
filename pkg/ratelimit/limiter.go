package ratelimit

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// clientVisitor tracks the rate limit state for a single IP.
type clientVisitor struct {
	tokens     float64
	lastSeen   time.Time
	capacity   float64
	refillRate float64 // tokens per second
}

// IPRateLimiter manages per-IP token bucket limiters.
type IPRateLimiter struct {
	visitors map[string]*clientVisitor
	mux      sync.Mutex
	rate     float64
	capacity float64
}

// NewIPRateLimiter creates a new rate limiter allowing `rate` requests per second with burst `capacity`.
func NewIPRateLimiter(rate float64, capacity float64) *IPRateLimiter {
	limiter := &IPRateLimiter{
		visitors: make(map[string]*clientVisitor),
		rate:     rate,
		capacity: capacity,
	}

	// Background cleanup of stale IP records every 3 minutes
	go limiter.cleanupStaleVisitors()

	return limiter
}

func (l *IPRateLimiter) getVisitor(ip string) *clientVisitor {
	l.mux.Lock()
	defer l.mux.Unlock()

	v, exists := l.visitors[ip]
	now := time.Now()

	if !exists {
		v = &clientVisitor{
			tokens:     l.capacity,
			lastSeen:   now,
			capacity:   l.capacity,
			refillRate: l.rate,
		}
		l.visitors[ip] = v
		return v
	}

	// Calculate refilled tokens based on elapsed time
	elapsed := now.Sub(v.lastSeen).Seconds()
	v.tokens += elapsed * v.refillRate
	if v.tokens > v.capacity {
		v.tokens = v.capacity
	}
	v.lastSeen = now

	return v
}

// Allow checks if the given IP address is allowed to proceed with a request.
func (l *IPRateLimiter) Allow(ip string) bool {
	v := l.getVisitor(ip)
	l.mux.Lock()
	defer l.mux.Unlock()

	if v.tokens >= 1.0 {
		v.tokens--
		return true
	}
	return false
}

func (l *IPRateLimiter) cleanupStaleVisitors() {
	ticker := time.NewTicker(3 * time.Minute)
	for range ticker.C {
		l.mux.Lock()
		for ip, v := range l.visitors {
			if time.Since(v.lastSeen) > 5*time.Minute {
				delete(l.visitors, ip)
			}
		}
		l.mux.Unlock()
	}
}

// Middleware returns an HTTP middleware enforcing rate limits.
func (l *IPRateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}

		if !l.Allow(host) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"Rate limit exceeded. Try again later."}` + "\n"))
			return
		}

		next.ServeHTTP(w, r)
	})
}
