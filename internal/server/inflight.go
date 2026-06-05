package server

import (
	"net/http"
	"sync/atomic"
)

// InFlightTracker counts active HTTP requests using atomic operations.
type InFlightTracker struct {
	active atomic.Int64
}

// NewInFlightTracker creates a new tracker.
func NewInFlightTracker() *InFlightTracker {
	return &InFlightTracker{}
}

// Inc increments the active request count.
func (t *InFlightTracker) Inc() { t.active.Add(1) }

// Dec decrements the active request count.
func (t *InFlightTracker) Dec() { t.active.Add(-1) }

// Count returns the current number of active requests.
func (t *InFlightTracker) Count() int64 { return t.active.Load() }

// Track wraps an http.Handler to count in-flight requests.
func (t *InFlightTracker) Track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Inc()
		defer t.Dec()
		next.ServeHTTP(w, r)
	})
}
