package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"oc-go-cc/internal/client"
	"oc-go-cc/internal/metrics"
	"oc-go-cc/internal/router"
	"oc-go-cc/internal/token"
	"oc-go-cc/pkg/types"
)

// HealthHandler handles health checks and token counting endpoints.
type HealthHandler struct {
	tokenCounter    *token.Counter
	fallbackHandler *router.FallbackHandler
	metrics         *metrics.Metrics
	openCodeClient  *client.OpenCodeClient
	tokenCountCache *client.ResponseCache
}

// NewHealthHandler creates a new health handler.
func NewHealthHandler(tokenCounter *token.Counter, fallbackHandler *router.FallbackHandler, metrics *metrics.Metrics, openCodeClient *client.OpenCodeClient) *HealthHandler {
	return &HealthHandler{
		tokenCounter:    tokenCounter,
		fallbackHandler: fallbackHandler,
		metrics:         metrics,
		openCodeClient:  openCodeClient,
		tokenCountCache: client.NewResponseCache(5 * time.Minute),
	}
}

// HandleHealth handles GET /health.
func (h *HealthHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	// Get metrics snapshot
	snapshot := h.metrics.GetSnapshot()

	// Get circuit breaker states
	cbStates := map[string]string{}
	if h.fallbackHandler != nil {
		cbStates = h.fallbackHandler.GetCircuitStates()
	}

	response := map[string]interface{}{
		"status":  "ok",
		"service": "oc-go-cc",
		"metrics": map[string]interface{}{
			"requests_received": snapshot.RequestsReceived,
			"requests_success":  snapshot.RequestsSuccess,
			"requests_failed":   snapshot.RequestsFailed,
			"requests_streamed": snapshot.RequestsStreamed,
			"upstream_calls":    snapshot.UpstreamCalls,
			"rate_limited":      snapshot.RateLimited,
			"deduplicated":      snapshot.Deduplicated,
			"p95_latency_ms":    snapshot.CalculateP95().Milliseconds(),
			"p99_latency_ms":    snapshot.CalculateP99().Milliseconds(),
		},
		"circuit_breakers": cbStates,
		"models":           snapshot.ModelCounts,
	}

	if h.openCodeClient != nil {
		response["keys"] = h.openCodeClient.HealthSnapshot()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

// HandleQuota handles GET /quota.
// Returns per-key request metrics (quota proxy) + health status.
func (h *HealthHandler) HandleQuota(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snapshot := h.metrics.GetSnapshot()
	cbStates := map[string]string{}
	if h.fallbackHandler != nil {
		cbStates = h.fallbackHandler.GetCircuitStates()
	}

	response := map[string]interface{}{
		"service":          "oc-go-cc",
		"status":           "ok",
		"circuit_breakers": cbStates,
		"models":           snapshot.ModelCounts,
		"metrics": map[string]interface{}{
			"requests_received": snapshot.RequestsReceived,
			"requests_success":  snapshot.RequestsSuccess,
			"requests_failed":   snapshot.RequestsFailed,
			"upstream_calls":    snapshot.UpstreamCalls,
			"rate_limited":      snapshot.RateLimited,
		},
	}

	if h.openCodeClient != nil {
		response["keys"] = h.openCodeClient.KeyMetricsSnapshot()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

func hashBytes(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// HandleCountTokens handles POST /v1/messages/count_tokens.
func (h *HealthHandler) HandleCountTokens(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body types.MessageRequest

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Cache key: SHA256 of the raw JSON body. Token counts are deterministic
	// for the same message content, so caching avoids redundant tiktoken work.
	bodyBytes, _ := json.Marshal(body)
	cacheKey := hashBytes(bodyBytes)
	if cached, ok := h.tokenCountCache.Get(cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "hit")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(cached)
		return
	}

	// Count tokens.
	systemText, err := systemAndToolsTokenText(body.SystemText(), body.Tools)
	if err != nil {
		http.Error(w, "failed to process tools", http.StatusBadRequest)
		return
	}
	messages := tokenMessagesFromAnthropic(body.Messages)
	count, err := h.tokenCounter.CountMessages(systemText, messages)
	if err != nil {
		http.Error(w, "failed to count tokens", http.StatusInternalServerError)
		return
	}

	resp := map[string]int{
		"input_tokens": count,
		"token_count":  count,
	}
	respBytes, _ := json.Marshal(resp)
	h.tokenCountCache.Set(cacheKey, respBytes)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "miss")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)
}
