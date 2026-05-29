package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"oc-go-cc/internal/config"
	"oc-go-cc/pkg/types"
)

func twoModelChain() []config.ModelConfig {
	return []config.ModelConfig{
		{ModelID: "primary"},
		{ModelID: "fallback"},
	}
}

// newHandler builds a fallback handler with a high circuit-breaker threshold
// so a single test failure never trips the breaker mid-test.
func newHandler() *FallbackHandler {
	return NewFallbackHandler(nil, 100, time.Minute)
}

func TestExecuteWithFallback_PreservesRateLimitError(t *testing.T) {
	h := newHandler()
	// primary 429s (quota), fallback fails with a generic error.
	// The terminal error MUST be the 429 (with Retry-After), not the generic one.
	exec := func(_ context.Context, m config.ModelConfig) ([]byte, error) {
		if m.ModelID == "primary" {
			return nil, &types.UpstreamError{StatusCode: 429, RetryAfter: "223920", Body: "quota"}
		}
		return nil, errors.New("connection reset")
	}

	_, _, err := h.ExecuteWithFallback(context.Background(), twoModelChain(), exec)
	if err == nil {
		t.Fatal("expected terminal error, got nil")
	}
	ue, ok := types.AsUpstreamError(err)
	if !ok {
		t.Fatalf("terminal error is not an UpstreamError: %v", err)
	}
	if ue.StatusCode != 429 || ue.RetryAfter != "223920" {
		t.Fatalf("rate-limit error not preserved: %+v", ue)
	}
}

func TestExecuteWithFallback_RateLimitPreferredOverLaterGeneric(t *testing.T) {
	h := newHandler()
	// primary generic-fails, fallback 429s. Either order must surface the 429.
	exec := func(_ context.Context, m config.ModelConfig) ([]byte, error) {
		if m.ModelID == "primary" {
			return nil, errors.New("timeout")
		}
		return nil, &types.UpstreamError{StatusCode: 429, RetryAfter: "60"}
	}

	_, _, err := h.ExecuteWithFallback(context.Background(), twoModelChain(), exec)
	if !types.IsRateLimited(err) {
		t.Fatalf("expected a 429 terminal error regardless of position, got: %v", err)
	}
}

func TestExecuteWithFallback_GenericWhenNoRateLimit(t *testing.T) {
	h := newHandler()
	exec := func(_ context.Context, _ config.ModelConfig) ([]byte, error) {
		return nil, &types.UpstreamError{StatusCode: 502, Body: "bad gateway"}
	}

	_, _, err := h.ExecuteWithFallback(context.Background(), twoModelChain(), exec)
	if err == nil {
		t.Fatal("expected terminal error, got nil")
	}
	if types.IsRateLimited(err) {
		t.Fatalf("non-429 failures must not be reported as rate-limited: %v", err)
	}
}

func TestExecuteWithFallback_SuccessShortCircuits(t *testing.T) {
	h := newHandler()
	calls := 0
	exec := func(_ context.Context, m config.ModelConfig) ([]byte, error) {
		calls++
		return []byte("ok"), nil
	}

	res, body, err := h.ExecuteWithFallback(context.Background(), twoModelChain(), exec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(body) != "ok" || !res.Success || calls != 1 {
		t.Fatalf("expected single successful call, got calls=%d res=%+v body=%q", calls, res, body)
	}
}
