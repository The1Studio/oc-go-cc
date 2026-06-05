package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRequestCoalescerFirstRequest(t *testing.T) {
	c := NewRequestCoalescer(time.Minute)
	body := json.RawMessage(`{"model":"test"}`)

	ifr, isFirst := c.Start(body)
	if !isFirst {
		t.Fatal("expected first request")
	}
	if ifr == nil {
		t.Fatal("expected non-nil in-flight request")
	}
}

func TestRequestCoalescerDuplicateWaits(t *testing.T) {
	c := NewRequestCoalescer(time.Minute)
	body := json.RawMessage(`{"model":"test"}`)

	_, isFirst := c.Start(body)
	if !isFirst {
		t.Fatal("expected first request")
	}

	ifr2, isFirst2 := c.Start(body)
	if isFirst2 {
		t.Fatal("expected duplicate to not be first")
	}

	// Simulate first request finishing.
	go func() {
		time.Sleep(20 * time.Millisecond)
		c.Finish(body, []byte(`{"ok":true}`), http.StatusOK, nil)
	}()

	// Waiter should receive the response.
	rec := httptest.NewRecorder()
	ifr2.Wait(rec)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); body != `{"ok":true}` {
		t.Fatalf("body = %q, want {\"ok\":true}", body)
	}
	if rec.Header().Get("X-Coalesced") != "true" {
		t.Fatal("expected X-Coalesced header")
	}
}

func TestRequestCoalescerDifferentBodiesAreIndependent(t *testing.T) {
	c := NewRequestCoalescer(time.Minute)
	body1 := json.RawMessage(`{"a":1}`)
	body2 := json.RawMessage(`{"a":2}`)

	_, isFirst1 := c.Start(body1)
	_, isFirst2 := c.Start(body2)

	if !isFirst1 || !isFirst2 {
		t.Fatal("expected both to be first requests (different bodies)")
	}
}
