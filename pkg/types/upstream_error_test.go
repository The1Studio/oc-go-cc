package types

import (
	"errors"
	"fmt"
	"testing"
)

func TestAsUpstreamError_Direct(t *testing.T) {
	ue := &UpstreamError{StatusCode: 429, RetryAfter: "223920", Body: "rate limited"}
	got, ok := AsUpstreamError(ue)
	if !ok {
		t.Fatal("AsUpstreamError returned ok=false for a direct *UpstreamError")
	}
	if got.StatusCode != 429 || got.RetryAfter != "223920" {
		t.Fatalf("unexpected unwrap: %+v", got)
	}
}

func TestAsUpstreamError_Wrapped(t *testing.T) {
	// The handler wraps client errors with %w ("chat completion failed: %w").
	// AsUpstreamError must see through the wrap.
	ue := &UpstreamError{StatusCode: 429, RetryAfter: "60"}
	wrapped := fmt.Errorf("chat completion failed: %w", ue)
	got, ok := AsUpstreamError(wrapped)
	if !ok {
		t.Fatal("AsUpstreamError failed to unwrap a %w-wrapped UpstreamError")
	}
	if got.RetryAfter != "60" {
		t.Fatalf("RetryAfter lost through wrap: %q", got.RetryAfter)
	}
}

func TestAsUpstreamError_NonUpstream(t *testing.T) {
	if _, ok := AsUpstreamError(errors.New("boom")); ok {
		t.Fatal("AsUpstreamError returned ok=true for a plain error")
	}
	if _, ok := AsUpstreamError(nil); ok {
		t.Fatal("AsUpstreamError returned ok=true for nil")
	}
}

func TestIsRateLimited(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"429 direct", &UpstreamError{StatusCode: 429}, true},
		{"429 wrapped", fmt.Errorf("x: %w", &UpstreamError{StatusCode: 429}), true},
		{"502 not rate-limited", &UpstreamError{StatusCode: 502}, false},
		{"401 not rate-limited", &UpstreamError{StatusCode: 401}, false},
		{"plain error", errors.New("nope"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRateLimited(tc.err); got != tc.want {
				t.Fatalf("IsRateLimited(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestUpstreamError_ErrorString(t *testing.T) {
	withRA := (&UpstreamError{StatusCode: 429, RetryAfter: "223920", Body: "limit"}).Error()
	if !contains(withRA, "429") || !contains(withRA, "223920") {
		t.Fatalf("Error() should mention status + retry-after, got: %q", withRA)
	}
	noRA := (&UpstreamError{StatusCode: 502, Body: "bad"}).Error()
	if contains(noRA, "retry-after") {
		t.Fatalf("Error() should omit retry-after when empty, got: %q", noRA)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
