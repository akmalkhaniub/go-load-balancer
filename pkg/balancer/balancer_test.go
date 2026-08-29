package balancer_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/akmalkhaniub/go-load-balancer/pkg/backend"
	"github.com/akmalkhaniub/go-load-balancer/pkg/balancer"
)

func TestRoundRobinDistribution(t *testing.T) {
	pool := balancer.NewServerPool(balancer.AlgorithmRoundRobin)

	// Create 3 dummy backends
	for i := 1; i <= 3; i++ {
		b, err := backend.NewBackend(fmt.Sprintf("http://127.0.0.%d:8080", i))
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		pool.AddBackend(b)
	}

	expectedOrder := []string{
		"http://127.0.0.2:8080",
		"http://127.0.0.3:8080",
		"http://127.0.0.1:8080",
		"http://127.0.0.2:8080",
	}

	for _, expected := range expectedOrder {
		b, err := pool.GetNextBackend("127.0.0.1")
		if err != nil {
			t.Fatalf("unexpected error getting backend: %v", err)
		}
		if b.URL.String() != expected {
			t.Errorf("expected %s, got %s", expected, b.URL.String())
		}
	}
}

func TestBackendFailureBypass(t *testing.T) {
	pool := balancer.NewServerPool(balancer.AlgorithmRoundRobin)

	b1, _ := backend.NewBackend("http://127.0.0.1:8081")
	b2, _ := backend.NewBackend("http://127.0.0.2:8082")

	pool.AddBackend(b1)
	pool.AddBackend(b2)

	// Mark b1 as down
	b1.SetAlive(false)

	for i := 0; i < 5; i++ {
		b, err := pool.GetNextBackend("127.0.0.1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if b.URL.String() != "http://127.0.0.2:8082" {
			t.Errorf("failed backend was not bypassed: got %s", b.URL.String())
		}
	}
}

func TestLeastConnectionsAlgorithm(t *testing.T) {
	pool := balancer.NewServerPool(balancer.AlgorithmLeastConnections)

	b1, _ := backend.NewBackend("http://127.0.0.1:8081")
	b2, _ := backend.NewBackend("http://127.0.0.2:8082")

	pool.AddBackend(b1)
	pool.AddBackend(b2)

	// Simulate 2 active connections on b1
	b1.IncrActive()
	b1.IncrActive()

	// b2 has 0 connections, so it should be picked
	b, err := pool.GetNextBackend("127.0.0.1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.URL.String() != "http://127.0.0.2:8082" {
		t.Errorf("expected b2 (least conns), got %s", b.URL.String())
	}
}

func TestConcurrentLoadBalancing(t *testing.T) {
	// Create mock HTTP backends
	var hitCount1, hitCount2 int64
	var mux1, mux2 sync.Mutex

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux1.Lock()
		hitCount1++
		mux1.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux2.Lock()
		hitCount2++
		mux2.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts2.Close()

	pool := balancer.NewServerPool(balancer.AlgorithmRoundRobin)
	b1, _ := backend.NewBackend(ts1.URL)
	b2, _ := backend.NewBackend(ts2.URL)
	pool.AddBackend(b1)
	pool.AddBackend(b2)

	var wg sync.WaitGroup
	requests := 100

	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			pool.ServeHTTP(w, req)
		}()
	}

	wg.Wait()

	if hitCount1+hitCount2 != int64(requests) {
		t.Fatalf("expected total %d requests, got %d", requests, hitCount1+hitCount2)
	}
}
