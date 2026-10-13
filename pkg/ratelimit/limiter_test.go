package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestIPRateLimiter_TokenConsumptionAndExhaustion(t *testing.T) {
	// Rate: 1 token/sec, Burst capacity: 3 tokens
	limiter := NewIPRateLimiter(1.0, 3.0)
	ip := "192.168.1.100"

	// First 3 requests should succeed (consuming 3 burst tokens)
	for i := 1; i <= 3; i++ {
		if !limiter.Allow(ip) {
			t.Fatalf("request #%d should have been allowed within burst capacity", i)
		}
	}

	// 4th request must be rejected immediately (tokens exhausted)
	if limiter.Allow(ip) {
		t.Fatal("request #4 should have been rate-limited")
	}
}

func TestIPRateLimiter_TokenRefill(t *testing.T) {
	// Rate: 10 tokens/sec, Capacity: 2 tokens
	limiter := NewIPRateLimiter(10.0, 2.0)
	ip := "10.0.0.1"

	// Exhaust burst
	limiter.Allow(ip)
	limiter.Allow(ip)
	if limiter.Allow(ip) {
		t.Fatal("expected request to be rate-limited after exhausting capacity")
	}

	// Wait 150ms -> should refill ~1.5 tokens (at 10 tokens/sec)
	time.Sleep(150 * time.Millisecond)

	if !limiter.Allow(ip) {
		t.Fatal("expected request to be allowed after token refill period")
	}
}

func TestIPRateLimiter_IPIsolation(t *testing.T) {
	limiter := NewIPRateLimiter(1.0, 2.0)
	ipA := "1.1.1.1"
	ipB := "2.2.2.2"

	// Exhaust ipA
	limiter.Allow(ipA)
	limiter.Allow(ipA)
	if limiter.Allow(ipA) {
		t.Fatal("ipA should be rate limited")
	}

	// ipB should still have full quota
	if !limiter.Allow(ipB) {
		t.Fatal("ipB should be allowed independently of ipA")
	}
	if !limiter.Allow(ipB) {
		t.Fatal("ipB should have second burst token available")
	}
}

func TestIPRateLimiter_Middleware(t *testing.T) {
	limiter := NewIPRateLimiter(1.0, 2.0)

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := limiter.Middleware(dummyHandler)

	// Make 2 allowed requests
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "172.16.0.5:12345"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
	}

	// 3rd request should be 429
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "172.16.0.5:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec.Code)
	}
}

func TestIPRateLimiter_ConcurrentAccess(t *testing.T) {
	limiter := NewIPRateLimiter(100.0, 50.0)
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ip := "192.168.1.1"
			for j := 0; j < 30; j++ {
				_ = limiter.Allow(ip)
			}
		}(i)
	}

	wg.Wait()
}
