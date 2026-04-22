// Package handlers contains HTTP request handlers for API endpoints.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"oc-go-cc/internal/client"
	"oc-go-cc/internal/config"
	"oc-go-cc/internal/metrics"
	"oc-go-cc/internal/middleware"
	"oc-go-cc/internal/router"
	"oc-go-cc/internal/telemetry"
	"oc-go-cc/internal/token"
	"oc-go-cc/internal/transformer"
	"oc-go-cc/pkg/types"
)

// MessagesHandler handles /v1/messages requests.
type MessagesHandler struct {
	config              *config.Config
	client              *client.OpenCodeClient
	modelRouter         *router.ModelRouter
	fallbackHandler     *router.FallbackHandler
	requestTransformer  *transformer.RequestTransformer
	responseTransformer *transformer.ResponseTransformer
	streamHandler       *transformer.StreamHandler
	tokenCounter        *token.Counter
	logger              *slog.Logger
	rateLimiter         *middleware.RateLimiter
	requestDedup        *middleware.RequestDeduplicator
	requestIDGen        *middleware.RequestIDGenerator
	metrics             *metrics.Metrics
	telemetryWriter     *telemetry.Writer
}

// responseWriter wraps http.ResponseWriter to track if headers were written.
type responseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *responseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Flush implements http.Flusher for SSE streaming support.
func (w *responseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// NewMessagesHandler creates a new messages handler.
func NewMessagesHandler(
	cfg *config.Config,
	openCodeClient *client.OpenCodeClient,
	modelRouter *router.ModelRouter,
	fallbackHandler *router.FallbackHandler,
	tokenCounter *token.Counter,
	metrics *metrics.Metrics,
	tw *telemetry.Writer,
) *MessagesHandler {
	return &MessagesHandler{
		config:              cfg,
		client:              openCodeClient,
		modelRouter:         modelRouter,
		fallbackHandler:     fallbackHandler,
		requestTransformer:  transformer.NewRequestTransformer(),
		responseTransformer: transformer.NewResponseTransformer(),
		streamHandler:       transformer.NewStreamHandler(),
		tokenCounter:        tokenCounter,
		logger:              slog.Default(),
		rateLimiter:         middleware.NewRateLimiter(100, time.Minute),
		requestDedup:        middleware.NewRequestDeduplicator(500 * time.Millisecond),
		requestIDGen:        middleware.NewRequestIDGenerator(),
		metrics:             metrics,
		telemetryWriter:     tw,
	}
}

// HandleMessages handles POST /v1/messages.
func (h *MessagesHandler) HandleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Generate or get request ID for correlation
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = h.requestIDGen.Generate()
	}
	w.Header().Set("X-Request-ID", requestID)

	// Rate limiting
	clientIP := middleware.GetClientIP(r)
	if !h.rateLimiter.Allow(clientIP) {
		h.metrics.RecordRateLimited()
		h.logger.Warn("rate limited", "client", clientIP, "request_id", requestID)
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}

	// Read the raw request body for debug logging
	var rawBody json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&rawBody); err != nil {
		h.sendError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	// Deduplicate - skip duplicate requests
	if _, ok := h.requestDedup.TryAcquire(rawBody); !ok {
		h.metrics.RecordDeduplicated()
		h.logger.Info("duplicate request skipped", "request_id", requestID)
		return
	}

	// Parse into Anthropic request
	var anthropicReq types.MessageRequest
	if err := json.Unmarshal(rawBody, &anthropicReq); err != nil {
		h.sendError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	// Validate request
	if err := anthropicReq.Validate(); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}

	// Record metrics
	isStreaming := anthropicReq.Stream != nil && *anthropicReq.Stream
	h.metrics.RecordRequest(isStreaming)

	h.logger.Info("received request",
		"model", anthropicReq.Model,
		"streaming", isStreaming,
		"messages", len(anthropicReq.Messages),
		"tools", len(anthropicReq.Tools),
		"max_tokens", anthropicReq.MaxTokens,
	)

	// Build message content for routing and token counting.
	var routerMessages []router.MessageContent
	var tokenMessages []token.MessageContent
	systemText := anthropicReq.SystemText()

	for _, msg := range anthropicReq.Messages {
		blocks := msg.ContentBlocks()
		content := extractTextFromBlocks(blocks)
		mc := router.MessageContent{
			Role:    msg.Role,
			Content: content,
		}
		routerMessages = append(routerMessages, mc)
		tokenMessages = append(tokenMessages, token.MessageContent{
			Role:    msg.Role,
			Content: content,
		})
	}

	// Count tokens.
	tokenCount, err := h.tokenCounter.CountMessages(systemText, tokenMessages)
	if err != nil {
		h.logger.Warn("failed to count tokens", "error", err)
		tokenCount = 0
	}

	// Route to appropriate model.
	// If the client requested a specific known model, use it directly (passthrough).
	// Otherwise, use scenario-based routing.
	var routeResult router.RouteResult
	var routeErr error
	routeResult, routeErr = h.modelRouter.RouteWithModel(anthropicReq.Model, routerMessages, tokenCount)
	if routeErr != nil {
		h.sendError(w, http.StatusInternalServerError, "routing failed", routeErr)
		return
	}

	h.logger.Info("routing request",
		"scenario", routeResult.Scenario,
		"model", routeResult.Primary.ModelID,
		"tokens", tokenCount,
	)

	// Build fallback chain.
	modelChain := routeResult.GetModelChain()

	if isStreaming {
		// Streaming: use ProxyStream for real-time SSE transformation
		h.handleStreaming(w, r, &anthropicReq, modelChain, rawBody, routeResult)
	} else {
		// Non-streaming: execute with fallback and return full response
		h.handleNonStreaming(w, r, &anthropicReq, modelChain, rawBody, routeResult)
	}
}

// handleStreaming handles a streaming request with real-time SSE proxying.
func (h *MessagesHandler) handleStreaming(
	w http.ResponseWriter,
	r *http.Request,
	anthropicReq *types.MessageRequest,
	modelChain []config.ModelConfig,
	rawBody json.RawMessage,
	routeResult router.RouteResult,
) {
	startTime := time.Now()

	// Each fallback attempt needs its own context with timeout.
	// Don't share r.Context() across fallbacks - when Claude Code retries,
	// the original context gets canceled and kills all fallbacks.
	clientCtx := r.Context()

	rw := &responseWriter{ResponseWriter: w}

	// Set SSE headers immediately so Claude Code knows the stream is alive.
	// This prevents client-side timeouts before we even start sending data.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// Start heartbeat to keep connection alive while waiting for upstream.
	// Claude Code times out after ~6 seconds of no data, so we send pings every 3 seconds
	// (frequent enough to prevent timeout, not so frequent as to cause overhead).
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Send SSE comment (ignored by client but keeps connection alive)
				fmt.Fprintf(w, ":keepalive\n\n")
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			case <-heartbeatDone:
				return
			case <-clientCtx.Done():
				return
			}
		}
	}()
	// Stop heartbeat when streaming completes
	defer close(heartbeatDone)

	streamStart := time.Now()
	attemptCount := 0
	var succeededModel string
	var lastErr error

	for _, model := range modelChain {
		// Check if client already disconnected before trying this model
		select {
		case <-clientCtx.Done():
			h.logger.Info("client disconnected, stopping streaming fallbacks")
			return
		default:
		}

		attemptCount++
		h.logger.Info("attempting streaming model", "model", model.ModelID)

		// Create a fresh context with timeout for THIS attempt only.
		// Don't use r.Context() directly - it gets canceled when Claude Code retries.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

		// Check if this is an Anthropic-native model (MiniMax)
		if client.IsAnthropicModel(model.ModelID) {
			// For MiniMax models, send raw Anthropic request to Anthropic endpoint
			// But we need to replace the model name in the raw body
			modelBody := replaceModelInRawBody(rawBody, model.ModelID)
			if err := h.handleAnthropicStreaming(ctx, rw, modelBody, model.ModelID); err != nil {
				cancel()
				lastErr = err
				// Check if this was a client disconnect
				if clientCtx.Err() == context.Canceled {
					h.logger.Info("client disconnected during anthropic stream")
					return
				}
				h.logger.Warn("anthropic streaming failed", "model", model.ModelID, "error", err)
				continue
			}
			cancel()
			succeededModel = model.ModelID
			latency := time.Since(streamStart)
			h.metrics.RecordSuccess(model.ModelID, latency)
			h.logger.Info("streaming completed", "model", model.ModelID, "latency", latency)

			// Emit telemetry event for successful streaming.
			h.emitTelemetry(anthropicReq, routeResult, succeededModel, true, attemptCount, startTime, 0, 0, 0, 0, "")
			return
		}

		// For OpenAI-compatible models, transform and send to OpenAI endpoint
		openaiReq, err := h.requestTransformer.TransformRequest(anthropicReq, model)
		if err != nil {
			cancel()
			lastErr = err
			h.logger.Warn("request transform failed", "model", model.ModelID, "error", err)
			continue
		}

		// Get streaming body from upstream
		streamBody, err := h.client.GetStreamingBody(ctx, model.ModelID, openaiReq)
		if err != nil {
			cancel()
			lastErr = err
			// Check if this was a client disconnect (context canceled)
			if clientCtx.Err() == context.Canceled {
				h.logger.Info("client disconnected during upstream request")
				return
			}
			h.logger.Warn("streaming request failed", "model", model.ModelID, "error", err)
			continue
		}

		// Proxy the stream: transform OpenAI SSE -> Anthropic SSE in real-time
		if err := h.streamHandler.ProxyStream(rw, streamBody, model.ModelID, clientCtx); err != nil {
			streamBody.Close()
			cancel()
			lastErr = err
			if err == transformer.ErrClientDisconnected {
				h.logger.Info("client disconnected during stream")
				return
			}
			// Check if this was a client disconnect
			if clientCtx.Err() == context.Canceled {
				h.logger.Info("client disconnected during stream (context canceled)")
				return
			}
			h.logger.Warn("stream proxy failed", "model", model.ModelID, "error", err)
			continue
		}

		streamBody.Close()
		cancel()
		succeededModel = model.ModelID
		latency := time.Since(streamStart)
		h.metrics.RecordSuccess(model.ModelID, latency)
		h.logger.Info("streaming completed", "model", model.ModelID, "latency", latency)

		// Emit telemetry event for successful streaming.
		h.emitTelemetry(anthropicReq, routeResult, succeededModel, true, attemptCount, startTime, 0, 0, 0, 0, "")
		return
	}

	// All models failed
	h.metrics.RecordFailure()

	errType := ""
	if lastErr != nil {
		errType = categorizeError(lastErr)
	}
	h.emitTelemetry(anthropicReq, routeResult, "", false, attemptCount, startTime, 0, 0, 0, 0, errType)

	if !rw.wroteHeader {
		h.sendError(w, http.StatusBadGateway, "all streaming models failed", nil)
	} else {
		// Headers already sent - send error as SSE event
		h.sendStreamError(rw, "all upstream models failed")
	}
}

// isClientDisconnected checks if the HTTP client has disconnected.
func isClientDisconnected(r *http.Request) bool {
	select {
	case <-r.Context().Done():
		return true
	default:
		return false
	}
}

// replaceModelInRawBody replaces the model field in raw JSON body with the actual model ID.
// This is needed for Anthropic endpoint which validates the model name.
func replaceModelInRawBody(rawBody json.RawMessage, modelID string) json.RawMessage {
	// Simple string replacement - find "model":"..." and replace with "model":"actual-model"
	bodyStr := string(rawBody)

	// Try to find and replace the model field
	// Pattern: "model":"claude-..." or "model":"any-model-name"
	if idx := strings.Index(bodyStr, `"model":"`); idx != -1 {
		start := idx + len(`"model":"`)
		if end := strings.Index(bodyStr[start:], `"`); end != -1 {
			oldModel := bodyStr[start : start+end]
			// Replace the model value
			newBody := bodyStr[:start] + modelID + bodyStr[start+end:]
			slog.Debug("replaced model in request body",
				"old_model", oldModel,
				"new_model", modelID,
				"success", true)
			return json.RawMessage(newBody)
		}
	}

	slog.Warn("could not find model field in request body, using original",
		"body_preview", bodyStr[:min(len(bodyStr), 200)])
	// If we couldn't parse, return original (will likely fail upstream but that's ok)
	return rawBody
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// handleAnthropicStreaming sends a raw Anthropic request to the Anthropic endpoint.
func (h *MessagesHandler) handleAnthropicStreaming(
	ctx context.Context,
	w http.ResponseWriter,
	rawBody json.RawMessage,
	modelID string,
) error {
	// Debug: Log what we're sending
	h.logger.Debug("sending anthropic streaming request",
		"model_id", modelID,
		"body_preview", string(rawBody)[:min(len(rawBody), 200)])

	// Send raw Anthropic request to Anthropic endpoint
	// Use ctx so cancellation propagates when client disconnects
	resp, err := h.client.SendAnthropicRequest(ctx, rawBody, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy the response directly (already in Anthropic format)
	// SSE headers already set by handleStreaming
	// Use io.Copy which handles streaming efficiently
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		// Check if this was a client disconnect
		if ctx.Err() == context.Canceled {
			return transformer.ErrClientDisconnected
		}
		return fmt.Errorf("failed to copy response: %w", err)
	}

	return nil
}

// sendStreamError sends an error event in the SSE stream.
// Use this when headers have already been written.
func (h *MessagesHandler) sendStreamError(w http.ResponseWriter, message string) {
	h.logger.Error("sending stream error", "message", message)

	errorEvent := map[string]interface{}{
		"type": "error",
		"error": map[string]interface{}{
			"type":    "api_error",
			"message": message,
		},
	}

	data, _ := json.Marshal(errorEvent)
	fmt.Fprintf(w, "event: error\ndata: %s\n\n", string(data))

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// handleNonStreaming handles a non-streaming request with fallback.
func (h *MessagesHandler) handleNonStreaming(
	w http.ResponseWriter,
	r *http.Request,
	anthropicReq *types.MessageRequest,
	modelChain []config.ModelConfig,
	rawBody json.RawMessage,
	routeResult router.RouteResult,
) {
	ctx := r.Context()
	startTime := time.Now()

	result, responseBody, err := h.fallbackHandler.ExecuteWithFallback(
		ctx,
		modelChain,
		func(ctx context.Context, model config.ModelConfig) ([]byte, error) {
			// Check if this is an Anthropic-native model (MiniMax)
			if client.IsAnthropicModel(model.ModelID) {
				return h.executeAnthropicRequest(ctx, rawBody, model)
			}
			// Otherwise use OpenAI transformation
			return h.executeOpenAIRequest(ctx, anthropicReq, model)
		},
	)

	if err != nil {
		h.metrics.RecordFailure()

		errType := categorizeError(err)
		h.emitTelemetry(anthropicReq, routeResult, "", false, result.Attempted, startTime, 0, 0, 0, 0, errType)

		h.sendError(w, http.StatusBadGateway, "all models failed", err)
		return
	}

	latency := time.Since(startTime)
	h.metrics.RecordSuccess(result.ModelID, latency)

	h.logger.Info("request completed",
		"model", result.ModelID,
		"attempts", result.Attempted,
		"latency", latency,
	)

	// Try to extract usage from the response for telemetry.
	inputTokens, outputTokens, cachedTokens, toolCallCount := extractUsageFromResponse(responseBody)

	// Determine fallback model if there were multiple attempts.
	fallbackModel := ""
	if result.Attempted > 1 {
		fallbackModel = result.ModelID
	}

	h.emitTelemetryFull(anthropicReq, routeResult, result.ModelID, true, result.Attempted,
		startTime, inputTokens, outputTokens, cachedTokens, toolCallCount, "", fallbackModel)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

// executeAnthropicRequest executes a request to the Anthropic endpoint (for MiniMax models).
func (h *MessagesHandler) executeAnthropicRequest(
	ctx context.Context,
	rawBody json.RawMessage,
	model config.ModelConfig,
) ([]byte, error) {
	// Send raw Anthropic request to Anthropic endpoint
	resp, err := h.client.SendAnthropicRequest(ctx, rawBody, false)
	if err != nil {
		return nil, fmt.Errorf("anthropic request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read the response (already in Anthropic format)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	h.logger.Debug("anthropic response", "body", string(body))

	return body, nil
}

// executeOpenAIRequest executes a request to the OpenAI endpoint with transformation.
func (h *MessagesHandler) executeOpenAIRequest(
	ctx context.Context,
	anthropicReq *types.MessageRequest,
	model config.ModelConfig,
) ([]byte, error) {
	// Transform request to OpenAI format.
	openaiReq, err := h.requestTransformer.TransformRequest(anthropicReq, model)
	if err != nil {
		return nil, fmt.Errorf("request transform failed: %w", err)
	}

	// Log the transformed request for debugging
	reqJSON, _ := json.Marshal(openaiReq)
	h.logger.Debug("transformed OpenAI request", "body", string(reqJSON))

	// Handle non-streaming.
	resp, err := h.client.ChatCompletionNonStreaming(ctx, model.ModelID, openaiReq)
	if err != nil {
		return nil, fmt.Errorf("chat completion failed: %w", err)
	}

	// Log the raw response for debugging
	respJSON, _ := json.Marshal(resp)
	h.logger.Debug("OpenAI response", "body", string(respJSON))

	// Transform response to Anthropic format.
	anthropicResp, err := h.responseTransformer.TransformResponse(resp, model.ModelID)
	if err != nil {
		return nil, fmt.Errorf("response transform failed: %w", err)
	}

	// Inject cached_tokens from OpenAI response into Anthropic usage for telemetry.
	if resp.Usage.PromptTokensDetails != nil && resp.Usage.PromptTokensDetails.CachedTokens > 0 {
		anthropicResp.Usage.CacheReadInputTokens = resp.Usage.PromptTokensDetails.CachedTokens
	}

	return json.Marshal(anthropicResp)
}

// extractTextFromBlocks extracts plain text from Anthropic content blocks.
func extractTextFromBlocks(blocks []types.ContentBlock) string {
	var content string
	for _, block := range blocks {
		switch block.Type {
		case "text":
			content += block.Text
		case "tool_use":
			content += fmt.Sprintf("[Tool Use: %s]", block.Name)
		case "tool_result":
			content += block.TextContent()
		case "thinking":
			// Skip thinking blocks for text extraction
		case "image":
			content += "[Image]"
		}
	}
	return content
}

// sendError sends an error response in Anthropic format.
// Safe to call multiple times - subsequent calls are no-ops.
func (h *MessagesHandler) sendError(w http.ResponseWriter, statusCode int, message string, err error) {
	h.logger.Error("request error",
		"status", statusCode,
		"message", message,
		"error", err,
	)

	// Use the wrapped writer if available to prevent duplicate WriteHeader calls
	if rw, ok := w.(*responseWriter); ok && rw.wroteHeader {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	errorResp := transformer.TransformErrorResponse(statusCode, message)
	json.NewEncoder(w).Encode(errorResp)
}

// --- Telemetry helpers ---

// emitTelemetry writes a telemetry event. Fail-open: panics/errors are caught.
func (h *MessagesHandler) emitTelemetry(
	req *types.MessageRequest,
	route router.RouteResult,
	routedModel string,
	success bool,
	attempts int,
	startTime time.Time,
	inputTokens, outputTokens, cachedTokens, toolCallCount int,
	errorType string,
) {
	h.emitTelemetryFull(req, route, routedModel, success, attempts, startTime,
		inputTokens, outputTokens, cachedTokens, toolCallCount, errorType, "")
}

// emitTelemetryFull writes a telemetry event with all fields including fallback model.
func (h *MessagesHandler) emitTelemetryFull(
	req *types.MessageRequest,
	route router.RouteResult,
	routedModel string,
	success bool,
	attempts int,
	startTime time.Time,
	inputTokens, outputTokens, cachedTokens, toolCallCount int,
	errorType string,
	fallbackModel string,
) {
	if h.telemetryWriter == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			h.logger.Warn("telemetry emit panic recovered", "error", r)
		}
	}()

	isStreaming := req.Stream != nil && *req.Stream
	if routedModel == "" {
		routedModel = route.Primary.ModelID
	}

	ev := telemetry.Event{
		RequestModel:     req.Model,
		RoutedModel:      routedModel,
		Scenario:         string(route.Scenario),
		Streaming:        isStreaming,
		MessageCount:     len(req.Messages),
		ToolDefCount:     len(req.Tools),
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		CachedTokens:     cachedTokens,
		LatencyMs:        time.Since(startTime).Milliseconds(),
		Success:          success,
		FallbackAttempts: attempts,
		FallbackModel:    fallbackModel,
		ErrorType:        errorType,
		ToolCallCount:    toolCallCount,
	}

	h.telemetryWriter.WriteEvent(ev)
}

// extractUsageFromResponse tries to extract token usage from a JSON response body.
// It checks both Anthropic and OpenAI response formats. Returns zeros on failure.
func extractUsageFromResponse(body []byte) (inputTokens, outputTokens, cachedTokens, toolCallCount int) {
	// Try Anthropic format first.
	var anthropicResp struct {
		Usage struct {
			InputTokens              int `json:"input_tokens"`
			OutputTokens             int `json:"output_tokens"`
			CacheReadInputTokens     int `json:"cache_read_input_tokens"`
			CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
		} `json:"usage"`
		Content []struct {
			Type string `json:"type"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &anthropicResp); err == nil && anthropicResp.Usage.InputTokens > 0 {
		inputTokens = anthropicResp.Usage.InputTokens
		outputTokens = anthropicResp.Usage.OutputTokens
		cachedTokens = anthropicResp.Usage.CacheReadInputTokens + anthropicResp.Usage.CacheCreationInputTokens
		for _, c := range anthropicResp.Content {
			if c.Type == "tool_use" {
				toolCallCount++
			}
		}
		return
	}

	// Try OpenAI format.
	var openaiResp struct {
		Usage struct {
			PromptTokens        int `json:"prompt_tokens"`
			CompletionTokens    int `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
		Choices []struct {
			Message struct {
				ToolCalls []struct{} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &openaiResp); err == nil && openaiResp.Usage.PromptTokens > 0 {
		inputTokens = openaiResp.Usage.PromptTokens
		outputTokens = openaiResp.Usage.CompletionTokens
		if openaiResp.Usage.PromptTokensDetails != nil {
			cachedTokens = openaiResp.Usage.PromptTokensDetails.CachedTokens
		}
		if len(openaiResp.Choices) > 0 {
			toolCallCount = len(openaiResp.Choices[0].Message.ToolCalls)
		}
	}

	return
}

// categorizeError returns a short error type string for telemetry.
func categorizeError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		return "timeout"
	case strings.Contains(s, "connection refused") || strings.Contains(s, "connection reset"):
		return "connection"
	case strings.Contains(s, "429") || strings.Contains(s, "rate limit"):
		return "rate_limit"
	case strings.Contains(s, "500") || strings.Contains(s, "502") || strings.Contains(s, "503"):
		return "server_error"
	case strings.Contains(s, "context canceled"):
		return "client_disconnect"
	default:
		return "unknown"
	}
}
