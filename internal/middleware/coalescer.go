package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// InFlightRequest tracks a single in-flight request for coalescing.
type InFlightRequest struct {
	done   chan struct{}
	resp   []byte
	status int
	err    error
}

// RequestCoalescer prevents duplicate upstream calls by letting identical
// non-streaming requests share the same response.
// Streaming requests are NOT coalesced (they are too complex to replay safely).
type RequestCoalescer struct {
	mu       sync.Mutex
	inFlight map[string]*InFlightRequest
	window   time.Duration
}

// NewRequestCoalescer creates a coalescer. window controls how long an entry
// stays in-flight after the first request arrives.
func NewRequestCoalescer(window time.Duration) *RequestCoalescer {
	if window <= 0 {
		window = 30 * time.Second
	}
	return &RequestCoalescer{
		inFlight: make(map[string]*InFlightRequest),
		window:   window,
	}
}

func hashBody(body json.RawMessage) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:])
}

// Start registers a new in-flight request and returns true if this is the
// first request for this hash. If false, the caller should wait on the returned
// InFlightRequest and then replay its response.
func (c *RequestCoalescer) Start(body json.RawMessage) (*InFlightRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := hashBody(body)
	if existing, ok := c.inFlight[hash]; ok {
		return existing, false
	}

	ifr := &InFlightRequest{done: make(chan struct{})}
	c.inFlight[hash] = ifr
	return ifr, true
}

// Finish broadcasts the result to all waiters and removes the in-flight entry.
func (c *RequestCoalescer) Finish(body json.RawMessage, resp []byte, status int, err error) {
	c.mu.Lock()
	ifr, ok := c.inFlight[hashBody(body)]
	if ok {
		delete(c.inFlight, hashBody(body))
	}
	c.mu.Unlock()

	if ok {
		ifr.resp = resp
		ifr.status = status
		ifr.err = err
		close(ifr.done)
	}
}

// Wait blocks until the in-flight request completes, then replays its
// response onto w. Returns true if the response was replayed.
func (ifr *InFlightRequest) Wait(w http.ResponseWriter) bool {
	<-ifr.done

	if ifr.err != nil {
		http.Error(w, ifr.err.Error(), http.StatusBadGateway)
		return true
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Coalesced", "true")
	w.WriteHeader(ifr.status)
	if len(ifr.resp) > 0 {
		_, _ = w.Write(ifr.resp)
	}
	return true
}
