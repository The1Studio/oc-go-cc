package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"oc-go-cc/internal/metrics"
	"oc-go-cc/internal/token"
)

func TestHandleCountTokensSupportsAnthropicContentBlocks(t *testing.T) {
	handler := newTestHealthHandler(t)

	body := []byte(`{
		"model":"deepseek-v4-pro",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello world"}]}]
	}`)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", bytes.NewReader(body))

	handler.HandleCountTokens(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("status = %d, want %d; body: %s", got, want, recorder.Body.String())
	}

	var response map[string]int
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("response is invalid JSON: %v", err)
	}
	if response["input_tokens"] <= 0 {
		t.Fatalf("input_tokens = %d, want positive", response["input_tokens"])
	}
	if got, want := response["token_count"], response["input_tokens"]; got != want {
		t.Fatalf("token_count = %d, want %d", got, want)
	}
	if recorder.Header().Get("X-Cache") != "miss" {
		t.Fatalf("expected X-Cache=miss on first call, got %q", recorder.Header().Get("X-Cache"))
	}
}

func TestHandleCountTokensCachesResult(t *testing.T) {
	handler := newTestHealthHandler(t)

	body := []byte(`{
		"model":"deepseek-v4-pro",
		"messages":[{"role":"user","content":[{"type":"text","text":"cache me"}]}]
	}`)

	// First call — cache miss.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", bytes.NewReader(body))
	handler.HandleCountTokens(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first call status = %d, want 200", rec1.Code)
	}
	if rec1.Header().Get("X-Cache") != "miss" {
		t.Fatalf("expected X-Cache=miss, got %q", rec1.Header().Get("X-Cache"))
	}

	// Second call with identical body — cache hit.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", bytes.NewReader(body))
	handler.HandleCountTokens(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second call status = %d, want 200", rec2.Code)
	}
	if rec2.Header().Get("X-Cache") != "hit" {
		t.Fatalf("expected X-Cache=hit, got %q", rec2.Header().Get("X-Cache"))
	}
	if rec2.Body.String() != rec1.Body.String() {
		t.Fatalf("cached body mismatch: %q vs %q", rec2.Body.String(), rec1.Body.String())
	}
}

func TestHandleCountTokensIncludesSystemToolsAndThinking(t *testing.T) {
	handler := newTestHealthHandler(t)

	base := countTokensForTest(t, handler, []byte(`{
		"model":"deepseek-v4-pro",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]
	}`))

	withContext := countTokensForTest(t, handler, []byte(`{
		"model":"deepseek-v4-pro",
		"system":[{"type":"text","text":"You are helpful"}],
		"tools":[{"name":"read_file","description":"Read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}}}}],
		"messages":[
			{"role":"assistant","content":[{"type":"thinking","thinking":"Need to inspect files"},{"type":"tool_use","id":"toolu_1","name":"read_file","input":{"path":"README.md"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"file contents"},{"type":"text","text":"continue"}]}
		]
	}`))

	if withContext <= base {
		t.Fatalf("context-rich count = %d, want greater than base %d", withContext, base)
	}
}

func countTokensForTest(t *testing.T, handler *HealthHandler, body []byte) int {
	t.Helper()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", bytes.NewReader(body))
	handler.HandleCountTokens(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("status = %d, want %d; body: %s", got, want, recorder.Body.String())
	}

	var response map[string]int
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("response is invalid JSON: %v", err)
	}
	return response["input_tokens"]
}

func TestHandleHealthOmitsKeysWhenClientNil(t *testing.T) {
	handler := newTestHealthHandler(t)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	handler.HandleHealth(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("status = %d, want %d; body: %s", got, want, recorder.Body.String())
	}

	var body map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := body["keys"]; ok {
		t.Fatal("expected keys field to be absent when client is nil")
	}
}

func TestHandleQuotaOmitsKeysWhenClientNil(t *testing.T) {
	handler := newTestHealthHandler(t)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/quota", nil)
	handler.HandleQuota(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("status = %d, want %d; body: %s", got, want, recorder.Body.String())
	}

	var body map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := body["keys"]; ok {
		t.Fatal("expected keys field to be absent when client is nil")
	}
	if body["service"] != "oc-go-cc" {
		t.Fatalf("service = %q, want oc-go-cc", body["service"])
	}
}

func TestHandleQuotaCachesTokenCountResults(t *testing.T) {
	handler := newTestHealthHandler(t)

	reqBody := []byte(`{"model":"deepseek-v4-pro","messages":[{"role":"user","content":"hello"}]}`)

	// Warm cache via count_tokens.
	rec1 := httptest.NewRecorder()
	handler.HandleCountTokens(rec1, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", bytes.NewReader(reqBody)))
	if rec1.Code != http.StatusOK {
		t.Fatalf("warm status = %d, want 200", rec1.Code)
	}

	// Same body again — should hit cache.
	rec2 := httptest.NewRecorder()
	handler.HandleCountTokens(rec2, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", bytes.NewReader(reqBody)))
	if rec2.Header().Get("X-Cache") != "hit" {
		t.Fatalf("expected cache hit, got %q", rec2.Header().Get("X-Cache"))
	}

	// Quota endpoint itself should still be dynamic.
	rec3 := httptest.NewRecorder()
	handler.HandleQuota(rec3, httptest.NewRequest(http.MethodGet, "/quota", nil))
	if rec3.Code != http.StatusOK {
		t.Fatalf("quota status = %d, want 200", rec3.Code)
	}
}

func newTestHealthHandler(t *testing.T) *HealthHandler {
	t.Helper()

	counter, err := token.NewCounter()
	if err != nil {
		t.Fatalf("NewCounter() error = %v", err)
	}
	return NewHealthHandler(counter, nil, metrics.New(), nil)
}
