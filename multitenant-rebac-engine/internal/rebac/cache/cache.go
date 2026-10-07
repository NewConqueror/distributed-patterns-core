package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Default TTL configurations
const (
	DefaultPositiveTTL = 10 * time.Minute
	DefaultNegativeTTL = 30 * time.Second
	DefaultL1Capacity  = 10000
)

// CacheEntry represents a cached authorization check decision.
type CacheEntry struct {
	Allowed   bool      `json:"allowed"`
	Version   int64     `json:"version"`
	ExpiresAt time.Time `json:"expires_at"`
}

// IsExpired checks if the cache entry has exceeded its time-to-live.
func (e CacheEntry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

// BuildRedisKey generates a Redis Cluster compatible hash-tagged key.
// Format: {tenant_id}:v{version}:rebac:check:{sha256(subject|permission|object)}
//
// Critical Architecture Note:
// The curly braces around "{tenant_id}" constitute a Redis Hash-Tag.
// Redis Cluster uses ONLY the contents of "{...}" to compute the CRC16 hash slot.
// This guarantees that all keys for the same tenant are co-located on the exact
// same Redis shard, enabling atomic pipelines and multi-key operations per tenant.
func BuildRedisKey(tenantID uuid.UUID, version int64, subject, permission, object string) string {
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%s|%s|%s", subject, permission, object)))
	contentHash := hex.EncodeToString(hasher.Sum(nil))

	return fmt.Sprintf("{%s}:v%d:rebac:check:%s", tenantID.String(), version, contentHash)
}

// BuildL1Key generates an in-memory key for local process cache.
func BuildL1Key(tenantID uuid.UUID, version int64, subject, permission, object string) string {
	return fmt.Sprintf("%s:v%d:%s:%s:%s", tenantID.String(), version, subject, permission, object)
}

// BuildTenantVersionKey returns the Redis key storing the current epoch/version of a tenant.
func BuildTenantVersionKey(tenantID uuid.UUID) string {
	return fmt.Sprintf("{%s}:version", tenantID.String())
}
