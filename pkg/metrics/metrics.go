package metrics

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/akmalkhaniub/go-load-balancer/pkg/backend"
)

// MetricsCollector tracks real-time traffic statistics.
type MetricsCollector struct {
	TotalRequests   uint64 `json:"total_requests"`
	TotalErrors     uint64 `json:"total_errors"`
	RateLimited     uint64 `json:"rate_limited_requests"`
	startTime       time.Time
	getBackendsFunc func() []*backend.Backend
}

// NewMetricsCollector initializes a metrics collector.
func NewMetricsCollector(getBackends func() []*backend.Backend) *MetricsCollector {
	return &MetricsCollector{
		startTime:       time.Now(),
		getBackendsFunc: getBackends,
	}
}

// IncrRequests increments total incoming requests.
func (m *MetricsCollector) IncrRequests() {
	atomic.AddUint64(&m.TotalRequests, 1)
}

// IncrErrors increments upstream or proxy error counter.
func (m *MetricsCollector) IncrErrors() {
	atomic.AddUint64(&m.TotalErrors, 1)
}

// IncrRateLimited increments rate limited counter.
func (m *MetricsCollector) IncrRateLimited() {
	atomic.AddUint64(&m.RateLimited, 1)
}

// BackendStat represents status of an individual upstream server.
type BackendStat struct {
	URL         string `json:"url"`
	Alive       bool   `json:"alive"`
	ActiveConns int64  `json:"active_connections"`
	TotalServed uint64 `json:"total_served"`
}

// Snapshot returns the current metrics status.
type Snapshot struct {
	UptimeSeconds float64       `json:"uptime_seconds"`
	TotalRequests uint64        `json:"total_requests"`
	TotalErrors   uint64        `json:"total_errors"`
	RateLimited   uint64        `json:"rate_limited"`
	Backends      []BackendStat `json:"backends"`
}

// Handler returns an HTTP handler serving JSON metrics.
func (m *MetricsCollector) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		backends := m.getBackendsFunc()
		stats := make([]BackendStat, 0, len(backends))

		for _, b := range backends {
			stats = append(stats, BackendStat{
				URL:         b.URL.String(),
				Alive:       b.IsAlive(),
				ActiveConns: b.GetActiveConns(),
				TotalServed: b.GetTotalServed(),
			})
		}

		snap := Snapshot{
			UptimeSeconds: time.Since(m.startTime).Seconds(),
			TotalRequests: atomic.LoadUint64(&m.TotalRequests),
			TotalErrors:   atomic.LoadUint64(&m.TotalErrors),
			RateLimited:   atomic.LoadUint64(&m.RateLimited),
			Backends:      stats,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	}
}
