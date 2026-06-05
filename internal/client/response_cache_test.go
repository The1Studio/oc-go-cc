package client

import (
	"testing"
	"time"
)

func TestResponseCacheGetMiss(t *testing.T) {
	c := NewResponseCache(time.Minute)
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected cache miss for unknown key")
	}
}

func TestResponseCacheGetHit(t *testing.T) {
	c := NewResponseCache(time.Minute)
	c.Set("k1", []byte("hello"))

	val, ok := c.Get("k1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(val) != "hello" {
		t.Fatalf("got %q, want hello", string(val))
	}
}

func TestResponseCacheReturnsCopy(t *testing.T) {
	c := NewResponseCache(time.Minute)
	original := []byte("hello")
	c.Set("k1", original)

	val, _ := c.Get("k1")
	val[0] = 'x'

	val2, _ := c.Get("k1")
	if string(val2) != "hello" {
		t.Fatal("cache value mutated — Get did not return a copy")
	}
}

func TestResponseCacheExpiresAfterTTL(t *testing.T) {
	c := NewResponseCache(50 * time.Millisecond)
	c.Set("k1", []byte("hello"))

	time.Sleep(100 * time.Millisecond)
	if _, ok := c.Get("k1"); ok {
		t.Fatal("expected expired entry to be evicted")
	}
}

func TestResponseCacheEvict(t *testing.T) {
	c := NewResponseCache(time.Minute)
	c.Set("k1", []byte("hello"))
	c.Evict("k1")
	if _, ok := c.Get("k1"); ok {
		t.Fatal("expected evicted entry to be gone")
	}
}
