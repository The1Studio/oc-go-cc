package transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"oc-go-cc/internal/config"
	"oc-go-cc/pkg/types"
)

// deepSeekThinkingModel is the live routing target for the failing class. Both
// ids in the production log (`deepseek-v4-flash`, `deepseek-flash`) satisfy
// isDeepSeekModel, so one stands in for both.
const deepSeekThinkingModel = "deepseek-v4-flash"

// streamedThinkingBody is turn 1 of a multi-turn conversation as DeepSeek's
// OpenAI-shaped upstream emits it: a reasoning burst, then visible text, then a
// tool call, then the terminal chunk. This is the exact shape oc-go-cc's
// streaming handler converts into Anthropic SSE.
func streamedThinkingBody() []string {
	return []string{
		`{"choices":[{"delta":{"reasoning_content":"The user wants the weather. I need the tool."}}]}`,
		`{"choices":[{"delta":{"content":"Let me check."}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"toolu_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Kigali\"}"}}]}}]}`,
		`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
	}
}

// replayAssistantTurn reconstructs the assistant message a client would persist
// after consuming an Anthropic SSE stream — content_block_start followed by the
// matching deltas. This mirrors Claude Code's own accumulation and is what the
// NEXT request replays back as history.
func replayAssistantTurn(t *testing.T, events []types.MessageEvent) types.Message {
	t.Helper()

	type accumulating struct {
		blockType string
		text      string
	}
	byIndex := map[int]*accumulating{}
	var order []int

	for _, ev := range events {
		switch ev.Type {
		case "content_block_start":
			if ev.ContentBlock == nil || ev.Index == nil {
				continue
			}
			if _, seen := byIndex[*ev.Index]; !seen {
				order = append(order, *ev.Index)
			}
			byIndex[*ev.Index] = &accumulating{blockType: ev.ContentBlock.Type}
		case "content_block_delta":
			if ev.Delta == nil || ev.Index == nil {
				continue
			}
			acc := byIndex[*ev.Index]
			if acc == nil {
				continue
			}
			switch ev.Delta.Type {
			case "thinking_delta":
				acc.text += ev.Delta.Thinking
			case "text_delta":
				acc.text += ev.Delta.Text
			}
		}
	}

	var blocks []map[string]interface{}
	for _, idx := range order {
		acc := byIndex[idx]
		switch acc.blockType {
		case "thinking":
			// Exactly what a client stores when the block carried no signature.
			blocks = append(blocks, map[string]interface{}{
				"type": "thinking", "thinking": acc.text, "signature": "",
			})
		case "text":
			blocks = append(blocks, map[string]interface{}{
				"type": "text", "text": acc.text,
			})
		case "tool_use":
			blocks = append(blocks, map[string]interface{}{
				"type": "tool_use", "id": "toolu_1", "name": "get_weather",
				"input": map[string]interface{}{"city": "Kigali"},
			})
		}
	}

	raw, err := json.Marshal(blocks)
	if err != nil {
		t.Fatalf("marshal replayed blocks: %v", err)
	}
	return types.Message{Role: "assistant", Content: raw}
}

// assistantReasoningPresence returns, for every assistant message in a
// serialized OpenAI request, whether `reasoning_content` is present and
// non-empty. Keyed by the assistant message's position in the output.
func assistantReasoningPresence(t *testing.T, openaiReq *types.ChatCompletionRequest) []bool {
	t.Helper()

	raw, err := json.Marshal(openaiReq)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var decoded struct {
		Messages []map[string]json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}

	var presence []bool
	for _, msg := range decoded.Messages {
		roleRaw, ok := msg["role"]
		if !ok {
			continue
		}
		var role string
		if err := json.Unmarshal(roleRaw, &role); err != nil || role != "assistant" {
			continue
		}
		rcRaw, ok := msg["reasoning_content"]
		present := false
		if ok {
			var rc string
			// Presence with a non-empty string is the contract DeepSeek
			// validates. A whitespace-only value is this package's deliberate
			// sentinel for "client stripped the original thinking" — see the
			// placeholder branches in request.go — so it counts as present.
			if err := json.Unmarshal(rcRaw, &rc); err == nil && rc != "" {
				present = true
			}
		}
		presence = append(presence, present)
	}
	return presence
}

// streamTextOnlyBody is an upstream turn that emitted NO reasoning at all —
// only visible text. DeepSeek does this on follow-up turns, and a client
// persists the result as a plain assistant text message carrying no thinking
// block.
func streamTextOnlyBody() []string {
	return []string{
		`{"choices":[{"delta":{"content":"It is sunny, 24C."}}]}`,
		`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
	}
}

// drainTurn runs one upstream SSE body through ProxyStream and returns the
// assistant message a client would persist from the emitted Anthropic events.
func drainTurn(t *testing.T, body []string) types.Message {
	t.Helper()

	handler := NewStreamHandler()
	w := newMockResponseWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := handler.ProxyStream(w, sseLines(body...), "claude-sonnet-4-5", ctx); err != nil {
		t.Fatalf("ProxyStream error: %v", err)
	}
	return replayAssistantTurn(t, parseSSEEvents(t, w.buf.String()))
}

// TestStreamingTurnTwoCarriesReasoningContent is the multi-turn streaming
// regression test for the production failure:
//
//	API Error: 400 Error from provider (Console Go): Upstream request failed:
//	[invalid_request_error] The `reasoning_content` in the thinking mode must be
//	passed back to the API.
//
// Two turns are streamed through ProxyStream and both replayed into a third
// request. Turn 1 emits reasoning and a tool call; turn 2 emits text only. Once
// any turn has carried thinking, thinking mode is active for the whole
// conversation and DeepSeek requires reasoning_content on EVERY assistant
// message — so the streamed text-only turn must still be replayed with the
// field, even though it has nothing of its own to carry.
func TestStreamingTurnTwoCarriesReasoningContent(t *testing.T) {
	turnOne := drainTurn(t, streamedThinkingBody())
	turnTwo := drainTurn(t, streamTextOnlyBody())

	// Turn 1 must have round-tripped its reasoning into a thinking block at all —
	// otherwise the failure under test is masked by an earlier drop.
	if !HasThinkingBlocks([]types.Message{turnOne}) {
		t.Fatal("streamed turn 1 produced no thinking block")
	}
	// Turn 2 must genuinely be the thinking-less shape under test.
	if HasThinkingBlocks([]types.Message{turnTwo}) {
		t.Fatal("streamed turn 2 unexpectedly carried a thinking block")
	}

	stream := true
	turnThree := &types.MessageRequest{
		Model:     "claude-sonnet-4-5",
		MaxTokens: 512,
		Stream:    &stream,
		Messages: []types.Message{
			{Role: "user", Content: json.RawMessage(`"What is the weather in Kigali?"`)},
			turnOne,
			{Role: "user", Content: json.RawMessage(
				`[{"type":"tool_result","tool_use_id":"toolu_1","content":"sunny, 24C"}]`)},
			turnTwo,
			{Role: "user", Content: json.RawMessage(`"Thanks."`)},
		},
	}

	openaiReq, err := NewRequestTransformer().TransformRequest(turnThree, config.ModelConfig{ModelID: deepSeekThinkingModel})
	if err != nil {
		t.Fatalf("TransformRequest() error = %v", err)
	}

	presence := assistantReasoningPresence(t, openaiReq)
	if len(presence) != 2 {
		t.Fatalf("expected 2 replayed assistant messages, got %d", len(presence))
	}
	for i, present := range presence {
		if !present {
			raw, _ := json.Marshal(openaiReq)
			t.Fatalf("replayed assistant message %d carries no reasoning_content; DeepSeek rejects this in thinking mode.\nrequest: %s", i, raw)
		}
	}
}

// TestNonStreamingPathStillCarriesReasoningContent is the non-streaming
// counterpart. The non-streaming path already worked in production and must
// keep working: an assistant turn whose thinking arrived as a real thinking
// block must still replay it verbatim, not as a placeholder.
func TestNonStreamingPathStillCarriesReasoningContent(t *testing.T) {
	stream := false
	req := &types.MessageRequest{
		Model:     "claude-sonnet-4-5",
		MaxTokens: 512,
		Stream:    &stream,
		Messages: []types.Message{
			{Role: "user", Content: json.RawMessage(`"What is the weather in Kigali?"`)},
			{Role: "assistant", Content: json.RawMessage(
				`[{"type":"thinking","thinking":"I need the weather tool.","signature":"sig_123"},
				  {"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Kigali"}}]`)},
			{Role: "user", Content: json.RawMessage(
				`[{"type":"tool_result","tool_use_id":"toolu_1","content":"sunny, 24C"}]`)},
		},
	}

	openaiReq, err := NewRequestTransformer().TransformRequest(req, config.ModelConfig{ModelID: deepSeekThinkingModel})
	if err != nil {
		t.Fatalf("TransformRequest() error = %v", err)
	}

	presence := assistantReasoningPresence(t, openaiReq)
	if len(presence) != 1 || !presence[0] {
		raw, _ := json.Marshal(openaiReq)
		t.Fatalf("non-streaming assistant turn lost its reasoning_content.\nrequest: %s", raw)
	}
	// The real thinking text must survive, not be replaced by a placeholder.
	if got := *openaiReq.Messages[1].ReasoningContent; got != "I need the weather tool." {
		t.Fatalf("ReasoningContent = %q, want the verbatim thinking text", got)
	}
}

// TestDeepSeekPlaceholderCoversTextOnlyAssistantTurn pins the gap the live log
// actually hits. Thinking mode stays enabled conversation-wide as soon as ANY
// assistant turn carried a thinking block (see HasThinkingBlocks), but Claude
// Code does not persist a thinking block on every assistant turn. A text-only
// assistant turn therefore reached DeepSeek with no reasoning_content and drew
// the 400.
func TestDeepSeekPlaceholderCoversTextOnlyAssistantTurn(t *testing.T) {
	stream := true
	req := &types.MessageRequest{
		Model:     "claude-sonnet-4-5",
		MaxTokens: 512,
		Stream:    &stream,
		Messages: []types.Message{
			{Role: "user", Content: json.RawMessage(`"What is the weather in Kigali?"`)},
			{Role: "assistant", Content: json.RawMessage(
				`[{"type":"thinking","thinking":"I need the weather tool.","signature":""},
				  {"type":"text","text":"Let me check."},
				  {"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Kigali"}}]`)},
			{Role: "user", Content: json.RawMessage(
				`[{"type":"tool_result","tool_use_id":"toolu_1","content":"sunny, 24C"}]`)},
			// Text-only assistant turn: no thinking block, no tool calls. This is
			// the shape the transcript shows at `router-4b52b578…` / `router-f347ab2c…`.
			{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"It is sunny, 24C."}]`)},
			{Role: "user", Content: json.RawMessage(`"Thanks."`)},
		},
	}

	openaiReq, err := NewRequestTransformer().TransformRequest(req, config.ModelConfig{ModelID: deepSeekThinkingModel})
	if err != nil {
		t.Fatalf("TransformRequest() error = %v", err)
	}

	// Thinking mode must be active for this test to mean anything.
	if openaiReq.Thinking == nil || !bytes.Contains(openaiReq.Thinking, []byte(`"enabled"`)) {
		t.Fatalf("thinking mode not enabled; Thinking = %s", openaiReq.Thinking)
	}

	presence := assistantReasoningPresence(t, openaiReq)
	if len(presence) != 2 {
		t.Fatalf("expected 2 assistant messages, got %d", len(presence))
	}
	for i, present := range presence {
		if !present {
			raw, _ := json.Marshal(openaiReq)
			t.Fatalf("assistant message %d carries no reasoning_content while thinking mode is enabled.\nrequest: %s", i, raw)
		}
	}
}

// TestNonThinkingModelHasNoSynthesisedReasoning guards the other direction: the
// DeepSeek completeness rule must not leak onto models that never entered
// thinking mode. A Kimi conversation with no thinking history must not gain a
// fabricated reasoning_content.
func TestNonThinkingModelHasNoSynthesisedReasoning(t *testing.T) {
	stream := true
	req := &types.MessageRequest{
		Model:     "claude-sonnet-4-5",
		MaxTokens: 512,
		Stream:    &stream,
		Messages: []types.Message{
			{Role: "user", Content: json.RawMessage(`"What is the weather in Kigali?"`)},
			{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"Let me check."}]`)},
			{Role: "user", Content: json.RawMessage(`"Thanks."`)},
		},
	}

	openaiReq, err := NewRequestTransformer().TransformRequest(req, config.ModelConfig{ModelID: "kimi-k2.6"})
	if err != nil {
		t.Fatalf("TransformRequest() error = %v", err)
	}

	if openaiReq.Thinking != nil {
		t.Fatalf("thinking mode enabled without thinking history: %s", openaiReq.Thinking)
	}
	for i, present := range assistantReasoningPresence(t, openaiReq) {
		if present {
			t.Fatalf("assistant message %d gained reasoning_content without thinking history", i)
		}
	}
}
