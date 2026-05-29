package client

import (
	"testing"
	"time"

	"oc-go-cc/internal/config"
)

func TestIsAnthropicModelOnlyRoutesNativeAnthropicModels(t *testing.T) {
	tests := []struct {
		name    string
		modelID string
		want    bool
	}{
		{
			name:    "minimax m2.5 uses anthropic endpoint",
			modelID: "minimax-m2.5",
			want:    true,
		},
		{
			name:    "minimax m2.7 uses anthropic endpoint",
			modelID: "minimax-m2.7",
			want:    true,
		},
		{
			name:    "deepseek pro uses openai endpoint",
			modelID: "deepseek-v4-pro",
			want:    false,
		},
		{
			name:    "deepseek flash uses openai endpoint",
			modelID: "deepseek-v4-flash",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAnthropicModel(tt.modelID); got != tt.want {
				t.Fatalf("IsAnthropicModel(%q) = %v, want %v", tt.modelID, got, tt.want)
			}
		})
	}
}

// newTestClient builds an OpenCodeClient backed by an in-memory atomic config.
// The HTTP client is never invoked by these key-selector tests so its defaults
// don't matter.
func newTestClient(cfg *config.Config) *OpenCodeClient {
	return NewOpenCodeClient(config.NewAtomicConfig(cfg, ""))
}

func TestPickKeyFallsBackToAPIKeyWhenAPIKeysEmpty(t *testing.T) {
	c := newTestClient(&config.Config{APIKey: "solo"})
	if got := c.pickKey(); got != "solo" {
		t.Fatalf("pickKey() = %q, want %q", got, "solo")
	}
}

func TestPickKeyRoundRobinsAcrossConfiguredKeys(t *testing.T) {
	c := newTestClient(&config.Config{APIKeys: []string{"k1", "k2", "k3"}})

	got := []string{c.pickKey(), c.pickKey(), c.pickKey(), c.pickKey()}
	want := []string{"k1", "k2", "k3", "k1"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pick[%d] = %q, want %q (sequence %v)", i, got[i], want[i], got)
		}
	}
}

func TestPickKeySkipsColdKeys(t *testing.T) {
	c := newTestClient(&config.Config{APIKeys: []string{"k1", "k2"}})

	c.markKeyCold("k1")

	for i := 0; i < 3; i++ {
		if got := c.pickKey(); got != "k2" {
			t.Fatalf("attempt %d: pickKey() = %q, want %q (k1 cold)", i, got, "k2")
		}
	}
}

func TestPickKeyReturnsEmptyWhenAllKeysCold(t *testing.T) {
	c := newTestClient(&config.Config{APIKeys: []string{"k1", "k2"}})

	c.markKeyCold("k1")
	c.markKeyCold("k2")

	if got := c.pickKey(); got != "" {
		t.Fatalf("pickKey() = %q, want empty (all cold)", got)
	}
}

func TestPickKeyRehabilitatesAfterCooldownExpires(t *testing.T) {
	c := newTestClient(&config.Config{APIKeys: []string{"k1"}})

	c.keyMu.Lock()
	c.keyCooldown["k1"] = time.Now().Add(-1 * time.Second) // already expired
	c.keyMu.Unlock()

	if got := c.pickKey(); got != "k1" {
		t.Fatalf("pickKey() = %q, want %q (cooldown expired)", got, "k1")
	}
}

func TestPickKeyReturnsEmptyWhenNoKeysConfigured(t *testing.T) {
	c := newTestClient(&config.Config{})
	if got := c.pickKey(); got != "" {
		t.Fatalf("pickKey() = %q, want empty (no keys)", got)
	}
}

func TestMarkKeyColdIgnoresEmptyString(t *testing.T) {
	c := newTestClient(&config.Config{APIKeys: []string{"k1"}})
	c.markKeyCold("") // must not panic; must not pollute cooldown map

	if got := c.pickKey(); got != "k1" {
		t.Fatalf("pickKey() = %q after no-op markKeyCold, want %q", got, "k1")
	}
}
