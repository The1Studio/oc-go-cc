package server

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestInFlightTrackerCount(t *testing.T) {
	tr := NewInFlightTracker()
	if tr.Count() != 0 {
		t.Fatalf("initial count = %d, want 0", tr.Count())
	}

	tr.Inc()
	if tr.Count() != 1 {
		t.Fatalf("after inc count = %d, want 1", tr.Count())
	}

	tr.Dec()
	if tr.Count() != 0 {
		t.Fatalf("after dec count = %d, want 0", tr.Count())
	}
}

func TestInFlightTrackerConcurrent(t *testing.T) {
	tr := NewInFlightTracker()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.Inc()
			time.Sleep(time.Millisecond)
			tr.Dec()
		}()
	}

	wg.Wait()
	if tr.Count() != 0 {
		t.Fatalf("final count = %d, want 0", tr.Count())
	}
}

func TestInFlightTrackerTrack(t *testing.T) {
	tr := NewInFlightTracker()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tr.Count() != 1 {
			t.Fatalf("during request count = %d, want 1", tr.Count())
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := tr.Track(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if tr.Count() != 0 {
		t.Fatalf("after request count = %d, want 0", tr.Count())
	}
}
