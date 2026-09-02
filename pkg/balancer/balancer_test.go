package balancer_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/akmalkhaniub/go-load-balancer/pkg/backend"
	"github.com/akmalkhaniub/go-load-balancer/pkg/balancer"
	"github.com/akmalkhaniub/go-load-balancer/pkg/circuit"
)

func TestRoundRobinDistribution(t *testing.T) {
	pool := balancer.NewServerPool(balancer.AlgorithmRoundRobin)

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

func TestCircuitBreakerTripping(t *testing.T) {
	cb := circuit.NewBreaker(2, 50*time.Millisecond)

	if !cb.Allow() {
		t.Fatal("expected circuit to be allowed initially")
	}

	// Trip circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.GetState() != circuit.StateOpen {
		t.Fatalf("expected state OPEN, got %s", cb.GetState())
	}
	if cb.Allow() {
		t.Fatal("expected OPEN circuit to reject requests")
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	// Now should transition to Half-Open on allow
	if !cb.Allow() {
		t.Fatal("expected probe request to be allowed after cooldown")
	}
	if cb.GetState() != circuit.StateHalfOpen {
		t.Fatalf("expected state HALF_OPEN, got %s", cb.GetState())
	}

	// Success probe
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.GetState() != circuit.StateClosed {
		t.Fatalf("expected state CLOSED after recovery, got %s", cb.GetState())
	}
}

func TestDynamicBackendRemovalAndDrain(t *testing.T) {
	pool := balancer.NewServerPool(balancer.AlgorithmRoundRobin)

	b1, _ := backend.NewBackend("http://127.0.0.1:8081")
	b2, _ := backend.NewBackend("http://127.0.0.2:8082")
	pool.AddBackend(b1)
	pool.AddBackend(b2)

	// Drain b1
	err := pool.DrainBackend("http://127.0.0.1:8081")
	if err != nil {
		t.Fatalf("unexpected drain error: %v", err)
	}

	// Next backend should now strictly be b2
	for i := 0; i < 3; i++ {
		b, err := pool.GetNextBackend("127.0.0.1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if b.URL.String() != "http://127.0.0.2:8082" {
			t.Errorf("drained backend was picked: got %s", b.URL.String())
		}
	}

	// Remove b1 completely
	err = pool.RemoveBackend("http://127.0.0.1:8081")
	if err != nil {
		t.Fatalf("unexpected remove error: %v", err)
	}

	if len(pool.GetBackends()) != 1 {
		t.Fatalf("expected 1 backend in pool, got %d", len(pool.GetBackends()))
	}
}

func TestConcurrentLoadBalancing(t *testing.T) {
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
