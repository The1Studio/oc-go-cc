package types

import (
	"errors"
	"fmt"
)

// UpstreamError is a structured error carrying the HTTP status and (when
// present) the Retry-After header returned by the OpenCode Go upstream.
//
// It exists so quota-exhaustion (HTTP 429 + Retry-After) survives the
// fallback loop instead of being flattened into a generic "all models
// failed" 502. Callers can errors.As() it to propagate the real status
// and reset hint back to the client.
type UpstreamError struct {
	StatusCode int
	RetryAfter string // raw Retry-After header value (seconds or HTTP-date); "" if absent
	Body       string
}

func (e *UpstreamError) Error() string {
	if e.RetryAfter != "" {
		return fmt.Sprintf("API error %d (retry-after: %s): %s", e.StatusCode, e.RetryAfter, e.Body)
	}
	return fmt.Sprintf("API error %d: %s", e.StatusCode, e.Body)
}

// AsUpstreamError unwraps err (following %w chains) into an *UpstreamError.
func AsUpstreamError(err error) (*UpstreamError, bool) {
	var ue *UpstreamError
	if errors.As(err, &ue) {
		return ue, true
	}
	return nil, false
}

// IsRateLimited reports whether err is an UpstreamError with status 429.
func IsRateLimited(err error) bool {
	ue, ok := AsUpstreamError(err)
	return ok && ue.StatusCode == 429
}
