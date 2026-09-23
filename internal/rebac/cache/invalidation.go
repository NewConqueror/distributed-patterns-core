package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const InvalidationChannel = "tenant_invalidations"

// InvalidationEvent is broadcast over Redis Pub/Sub when a relationship tuple changes.
type InvalidationEvent struct {
	TenantID       string `json:"tenant_id"`
	AffectedEntity string `json:"affected_entity,omitempty"`
	Timestamp      int64  `json:"timestamp"`
}

// InvalidationBus coordinates distributed cache invalidation across all application pods.
type InvalidationBus struct {
	redisClient *redis.Client
	l1Cache     *L1Cache
	l2Cache     *L2Cache
}

// NewInvalidationBus creates a new invalidation coordinator.
func NewInvalidationBus(client *redis.Client, l1 *L1Cache, l2 *L2Cache) *InvalidationBus {
	return &InvalidationBus{
		redisClient: client,
		l1Cache:     l1,
		l2Cache:     l2,
	}
}

// InvalidateTenant performs two-tier synchronized invalidation:
// 1. Zookies approach: Atomically increments the tenant version in Redis (O(1) L2 invalidation).
// 2. Pub/Sub approach: Broadcasts event to all instances to purge their local L1 caches.
func (b *InvalidationBus) InvalidateTenant(ctx context.Context, tenantID uuid.UUID, affectedEntity string) error {
	// 1. O(1) Ephemeral Version Increment in Redis
	newVersion, err := b.l2Cache.IncrementTenantVersion(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("failed to increment tenant version during invalidation: %w", err)
	}

	// 2. Local L1 Purge
	if b.l1Cache != nil {
		b.l1Cache.Purge()
	}

	// 3. Broadcast to all other pods via Redis Pub/Sub
	event := InvalidationEvent{
		TenantID:       tenantID.String(),
		AffectedEntity: affectedEntity,
		Timestamp:      time.Now().UnixNano(),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal invalidation event: %w", err)
	}

	if err := b.redisClient.Publish(ctx, InvalidationChannel, data).Err(); err != nil {
		return fmt.Errorf("failed to publish invalidation event to redis: %w", err)
	}

	log.Printf("[InvalidationBus] Tenant %s invalidated -> New Version: %d, Entity: %s",
		tenantID, newVersion, affectedEntity)

	return nil
}

// StartSubscriber starts a background listener for Redis Pub/Sub invalidation events.
func (b *InvalidationBus) StartSubscriber(ctx context.Context) {
	pubsub := b.redisClient.Subscribe(ctx, InvalidationChannel)

	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}

				var event InvalidationEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					log.Printf("[InvalidationBus] Failed to parse event: %v", err)
					continue
				}

				// Purge local L1 cache to remove stale decisions
				if b.l1Cache != nil {
					b.l1Cache.Purge()
				}

				log.Printf("[InvalidationBus] Received invalidation for tenant %s, purged local L1 cache", event.TenantID)
			}
		}
	}()
}
