package circuit

import (
	"sync"
	"time"
)

// State represents the current lifecycle state of a circuit breaker.
type State string

const (
	StateClosed   State = "CLOSED"    // Normal operation: traffic flows through
	StateOpen     State = "OPEN"      // Tripped: traffic is rejected immediately to protect backend
	StateHalfOpen State = "HALF_OPEN" // Trial state: limited probes allowed to test recovery
)

// Breaker implements the Circuit Breaker pattern to protect upstream servers from cascading overload.
type Breaker struct {
	maxFailures     int
	timeout         time.Duration
	state           State
	failureCount    int
	consecutivePass int
	lastStateChange time.Time
	mux             sync.RWMutex
}

// NewBreaker creates a new circuit breaker with failure threshold and cooldown timeout.
func NewBreaker(maxFailures int, timeout time.Duration) *Breaker {
	return &Breaker{
		maxFailures:     maxFailures,
		timeout:         timeout,
		state:           StateClosed,
		lastStateChange: time.Now(),
	}
}

// Allow determines if a request should be forwarded or rejected immediately.
func (b *Breaker) Allow() bool {
	b.mux.Lock()
	defer b.mux.Unlock()

	now := time.Now()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		// If cooldown timeout has elapsed, transition to Half-Open to test upstream
		if now.Sub(b.lastStateChange) >= b.timeout {
			b.state = StateHalfOpen
			b.lastStateChange = now
			b.consecutivePass = 0
			return true
		}
		return false
	case StateHalfOpen:
		// In Half-Open, allow single probe attempt
		return true
	default:
		return true
	}
}

// RecordSuccess registers a successful upstream response.
func (b *Breaker) RecordSuccess() {
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.state == StateHalfOpen {
		b.consecutivePass++
		// If probe succeeds, reset back to Closed
		if b.consecutivePass >= 2 {
			b.state = StateClosed
			b.failureCount = 0
			b.lastStateChange = time.Now()
		}
	} else if b.state == StateClosed {
		b.failureCount = 0
	}
}

// RecordFailure registers an upstream error (5xx / TCP timeout) and trips circuit if threshold exceeded.
func (b *Breaker) RecordFailure() {
	b.mux.Lock()
	defer b.mux.Unlock()

	b.failureCount++
	b.consecutivePass = 0

	if b.state == StateHalfOpen || b.failureCount >= b.maxFailures {
		b.state = StateOpen
		b.lastStateChange = time.Now()
	}
}

// GetState returns the current state of the circuit breaker.
func (b *Breaker) GetState() State {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.state
}
