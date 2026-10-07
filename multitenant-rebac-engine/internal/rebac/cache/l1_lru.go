package cache

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// L1Cache is an in-memory process-level LRU cache.
// Typical latency: ~100 nanoseconds.
type L1Cache struct {
	lru   *lru.Cache[string, CacheEntry]
	mutex sync.RWMutex
}

// NewL1Cache initializes a thread-safe LRU cache with the given capacity.
func NewL1Cache(capacity int) (*L1Cache, error) {
	if capacity <= 0 {
		capacity = DefaultL1Capacity
	}
	cache, err := lru.New[string, CacheEntry](capacity)
	if err != nil {
		return nil, err
	}
	return &L1Cache{lru: cache}, nil
}

// Get retrieves an entry from L1 cache. If expired, it removes it and returns false.
func (c *L1Cache) Get(key string) (bool, bool) {
	c.mutex.RLock()
	entry, found := c.lru.Get(key)
	c.mutex.RUnlock()

	if !found {
		return false, false
	}

	if entry.IsExpired() {
		c.mutex.Lock()
		c.lru.Remove(key)
		c.mutex.Unlock()
		return false, false
	}

	return entry.Allowed, true
}

// Set stores a decision in L1 cache with the calculated TTL.
func (c *L1Cache) Set(key string, allowed bool, version int64, ttl time.Duration) {
	entry := CacheEntry{
		Allowed:   allowed,
		Version:   version,
		ExpiresAt: time.Now().Add(ttl),
	}
	c.mutex.Lock()
	c.lru.Add(key, entry)
	c.mutex.Unlock()
}

// Invalidate removes a specific key from the cache.
func (c *L1Cache) Invalidate(key string) {
	c.mutex.Lock()
	c.lru.Remove(key)
	c.mutex.Unlock()
}

// Purge clears all entries in L1 cache (e.g. on global invalidation).
func (c *L1Cache) Purge() {
	c.mutex.Lock()
	c.lru.Purge()
	c.mutex.Unlock()
}

// Len returns current number of cached items in L1.
func (c *L1Cache) Len() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.lru.Len()
}
