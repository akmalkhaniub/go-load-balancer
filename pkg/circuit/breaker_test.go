package circuit

import (
	"sync"
	"testing"
	"time"
)

func TestBreaker_InitialState(t *testing.T) {
	b := NewBreaker(3, 50*time.Millisecond)
	if b.GetState() != StateClosed {
		t.Fatalf("expected initial state CLOSED, got %s", b.GetState())
	}
	if !b.Allow() {
		t.Fatal("expected Allow() to return true in CLOSED state")
	}
}

func TestBreaker_TripToOpen(t *testing.T) {
	maxFailures := 3
	b := NewBreaker(maxFailures, 50*time.Millisecond)

	// Record 2 failures - should stay closed
	b.RecordFailure()
	b.RecordFailure()
	if b.GetState() != StateClosed {
		t.Fatalf("expected state CLOSED after 2 failures, got %s", b.GetState())
	}

	// 3rd failure trips the breaker
	b.RecordFailure()
	if b.GetState() != StateOpen {
		t.Fatalf("expected state OPEN after %d failures, got %s", maxFailures, b.GetState())
	}

	// Immediate requests must be rejected
	if b.Allow() {
		t.Fatal("expected Allow() to return false in OPEN state before timeout")
	}
}

func TestBreaker_HalfOpenAndRecovery(t *testing.T) {
	cooldown := 30 * time.Millisecond
	b := NewBreaker(2, cooldown)

	// Trip to OPEN
	b.RecordFailure()
	b.RecordFailure()
	if b.GetState() != StateOpen {
		t.Fatalf("expected OPEN state, got %s", b.GetState())
	}

	// Wait for cooldown
	time.Sleep(cooldown + 10*time.Millisecond)

	// Allow should transition to HALF_OPEN
	if !b.Allow() {
		t.Fatal("expected Allow() to return true after cooldown, entering HALF_OPEN")
	}
	if b.GetState() != StateHalfOpen {
		t.Fatalf("expected HALF_OPEN state, got %s", b.GetState())
	}

	// Record first success - still HALF_OPEN
	b.RecordSuccess()
	if b.GetState() != StateHalfOpen {
		t.Fatalf("expected HALF_OPEN after 1 success, got %s", b.GetState())
	}

	// Record second success - should recover to CLOSED
	b.RecordSuccess()
	if b.GetState() != StateClosed {
		t.Fatalf("expected CLOSED state after 2 consecutive passes, got %s", b.GetState())
	}
}

func TestBreaker_HalfOpenFailureTripsImmediately(t *testing.T) {
	cooldown := 30 * time.Millisecond
	b := NewBreaker(3, cooldown)

	// Trip
	b.RecordFailure()
	b.RecordFailure()
	b.RecordFailure()

	// Wait cooldown & trigger HALF_OPEN
	time.Sleep(cooldown + 10*time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected Allow() to return true")
	}

	// A single failure in HALF_OPEN should immediately re-trip to OPEN
	b.RecordFailure()
	if b.GetState() != StateOpen {
		t.Fatalf("expected OPEN state immediately after failure in HALF_OPEN, got %s", b.GetState())
	}
}

func TestBreaker_ConcurrentAccess(t *testing.T) {
	b := NewBreaker(10, 50*time.Millisecond)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if b.Allow() {
					if id%3 == 0 {
						b.RecordFailure()
					} else {
						b.RecordSuccess()
					}
				}
				_ = b.GetState()
			}
		}(i)
	}

	wg.Wait()
}
