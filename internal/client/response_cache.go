package client

import (
	"sync"
	"time"
)

// ResponseCache is a thread-safe TTL cache for arbitrary byte-slice values.
// Replaces the orphaned QuotaCache; used for token-count caching and
// upstream non-streaming response caching.
type ResponseCache struct {
	mu    sync.RWMutex
	items map[string]cacheEntry
	ttl   time.Duration
}

type cacheEntry struct {
	data     []byte
	cachedAt time.Time
}

// NewResponseCache creates a new TTL cache. ttl must be > 0.
func NewResponseCache(ttl time.Duration) *ResponseCache {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &ResponseCache{
		items: make(map[string]cacheEntry),
		ttl:   ttl,
	}
}

// Get returns cached data if still valid.
func (c *ResponseCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	ent, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if time.Since(ent.cachedAt) > c.ttl {
		c.mu.Lock()
		delete(c.items, key)
		c.mu.Unlock()
		return nil, false
	}
	// Return a copy so callers cannot mutate the internal slice.
	out := make([]byte, len(ent.data))
	copy(out, ent.data)
	return out, true
}

// Set stores data with the current timestamp.
func (c *ResponseCache) Set(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Store a copy so callers retain ownership of the original slice.
	cp := make([]byte, len(data))
	copy(cp, data)
	c.items[key] = cacheEntry{data: cp, cachedAt: time.Now()}
}

// Evict removes a specific key.
func (c *ResponseCache) Evict(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}
