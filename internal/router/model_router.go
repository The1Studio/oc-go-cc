// Package router defines HTTP route registration and middleware chaining,
// as well as model selection based on request scenarios.
package router

import (
	"fmt"

	"oc-go-cc/internal/config"
)

// ModelRouter handles model selection based on scenarios.
type ModelRouter struct {
	config *config.Config
}

// NewModelRouter creates a new model router.
func NewModelRouter(cfg *config.Config) *ModelRouter {
	return &ModelRouter{config: cfg}
}

// RouteResult contains the selected model and fallback chain.
type RouteResult struct {
	Primary   config.ModelConfig
	Fallbacks []config.ModelConfig
	Scenario  Scenario
}

// KnownOpenCodeGoModels lists model IDs that can be passed through directly.
var KnownOpenCodeGoModels = map[string]bool{
	"glm-5.1":       true,
	"glm-5":         true,
	"kimi-k2.6":     true,
	"kimi-k2.5":     true,
	"mimo-v2-pro":   true,
	"mimo-v2-omni":  true,
	"minimax-m2.7":  true,
	"minimax-m2.5":  true,
	"qwen3.6-plus":  true,
	"qwen3.5-plus":  true,
}

// RouteWithModel determines which model to use, respecting client's model choice
// when it matches a known OpenCode Go model (passthrough mode).
func (r *ModelRouter) RouteWithModel(requestModel string, messages []MessageContent, tokenCount int) (RouteResult, error) {
	// Passthrough: if the client explicitly requested a known model, use it directly
	if requestModel != "" && KnownOpenCodeGoModels[requestModel] {
		return RouteResult{
			Primary: config.ModelConfig{
				Provider:  "opencode-go",
				ModelID:   requestModel,
				MaxTokens: 4096,
			},
			Fallbacks: r.config.Fallbacks["default"],
			Scenario:  ScenarioPassthrough,
		}, nil
	}

	// Otherwise, use scenario-based routing
	return r.Route(messages, tokenCount)
}

// Route determines which model to use for a request.
func (r *ModelRouter) Route(messages []MessageContent, tokenCount int) (RouteResult, error) {
	result := DetectScenario(messages, tokenCount, r.config)

	// Get primary model for scenario
	primary, ok := r.config.Models[string(result.Scenario)]
	if !ok {
		// Fall back to default if scenario model not configured
		primary, ok = r.config.Models["default"]
		if !ok {
			return RouteResult{}, fmt.Errorf("no default model configured")
		}
	}

	// Get fallbacks for scenario
	fallbacks := r.config.Fallbacks[string(result.Scenario)]
	if len(fallbacks) == 0 {
		// Fall back to default fallbacks
		fallbacks = r.config.Fallbacks["default"]
	}

	return RouteResult{
		Primary:   primary,
		Fallbacks: fallbacks,
		Scenario:  result.Scenario,
	}, nil
}

// GetModelChain returns the full chain of models to try (primary + fallbacks).
func (rr *RouteResult) GetModelChain() []config.ModelConfig {
	chain := []config.ModelConfig{rr.Primary}
	chain = append(chain, rr.Fallbacks...)
	return chain
}

// RouteForStreaming determines which model to use for streaming requests.
// Prioritizes fast TTFT (time-to-first-token) over capability.
func (r *ModelRouter) RouteForStreaming(messages []MessageContent, tokenCount int) RouteResult {
	result := RouteForStreaming(messages, tokenCount, r.config)

	// Get primary model for scenario
	primary, ok := r.config.Models[string(result.Scenario)]
	if !ok {
		// Fall back to fast scenario if not configured
		primary, ok = r.config.Models["fast"]
		if !ok {
			// Fall back to default
			primary = r.config.Models["default"]
		}
	}

	// Get fallbacks for scenario
	fallbacks := r.config.Fallbacks[string(result.Scenario)]
	if len(fallbacks) == 0 {
		// Fall back to fast fallbacks
		fallbacks = r.config.Fallbacks["fast"]
	}

	return RouteResult{
		Primary:   primary,
		Fallbacks: fallbacks,
		Scenario:  result.Scenario,
	}
}
