package database

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"flashsale/internal/config"
	"github.com/redis/go-redis/v9"
)

const OrderStreamKey = "orders:stream"

const defaultDecrStockLua = `local stock = redis.call('GET', KEYS[1])
if not stock then
    return -1 -- Ürün bulunamadı
end
if tonumber(stock) <= 0 then
    return 0 -- Stok bitti
end

-- 1. Atomik stok düşümü
redis.call('DECR', KEYS[1])

-- 2. Atomik olarak Redis Stream'e rezervasyon mesajı yaz (Dual-Write problemini sıfırlar!)
redis.call('XADD', KEYS[2], '*', 
    'reservation_id', ARGV[1], 
    'product_id', ARGV[2], 
    'user_id', ARGV[3], 
    'created_at', ARGV[4]
)

-- 3. Rezervasyon TTL anahtarı oluştur (Zaman aşımı / Rollback için: order:{reservation_id}:ttl -> product_id)
local ttl = tonumber(ARGV[5])
if ttl and ttl > 0 then
    redis.call('SET', 'order:' .. ARGV[1] .. ':ttl', ARGV[2], 'EX', ttl)
end

return 1 -- Başarılı
`

type ReservationEvent struct {
	StreamID      string
	ReservationID string
	ProductID     int
	UserID        string
	CreatedAt     time.Time
}

type RedisService struct {
	Client       *redis.Client
	decrStockSHA string
}

func NewRedisService(cfg *config.Config) (*RedisService, error) {
	addr := cfg.GetRedisAddr()
	log.Printf("[REDIS] Connecting to Redis at %s...\n", addr)

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.RedisPassword,
		DB:       0,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	log.Println("[REDIS] Successfully connected to Redis.")

	// Read Lua script from file if exists, otherwise fallback to embedded
	scriptContent := defaultDecrStockLua
	if content, err := os.ReadFile("scripts/lua/decr_stock.lua"); err == nil {
		scriptContent = string(content)
	}

	// SCRIPT LOAD into Redis and obtain SHA1
	sha, err := rdb.ScriptLoad(ctx, scriptContent).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load Lua script into Redis: %w", err)
	}

	log.Printf("[REDIS] Loaded decr_stock.lua (SHA: %s)\n", sha)

	svc := &RedisService{
		Client:       rdb,
		decrStockSHA: sha,
	}

	// Initialize default product:1:stock = 100 if not exists
	_ = svc.InitProductStock(ctx, 1, 100)

	return svc, nil
}

// Key format: product:{id}:stock
func ProductStockKey(productID int) string {
	return fmt.Sprintf("product:%d:stock", productID)
}

func (s *RedisService) InitProductStock(ctx context.Context, productID, stock int) error {
	key := ProductStockKey(productID)
	return s.Client.Set(ctx, key, stock, 0).Err()
}

func (s *RedisService) GetProductStock(ctx context.Context, productID int) (int, error) {
	key := ProductStockKey(productID)
	val, err := s.Client.Get(ctx, key).Int()
	if err != nil {
		return 0, err
	}
	return val, nil
}

// DecrStockAndEnqueue executes the atomic Lua script via EVALSHA.
// It decrements stock, pushes the reservation to Redis Stream, AND sets the order TTL key
// in the exact same atomic transaction, guaranteeing zero dual-write discrepancy.
//
// Returns:
//
//	 1: Stock successfully decremented, event enqueued, TTL key created
//	 0: Out of stock (stock <= 0)
//	-1: Product key not found in Redis
func (s *RedisService) DecrStockAndEnqueue(ctx context.Context, productID int, reservationID, userID string, ttlSeconds int) (int64, error) {
	if ttlSeconds <= 0 {
		ttlSeconds = 600 // Default 10 minutes (600s)
	}

	productKey := ProductStockKey(productID)
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	ttlStr := strconv.Itoa(ttlSeconds)

	res, err := s.Client.EvalSha(ctx, s.decrStockSHA, []string{productKey, OrderStreamKey}, reservationID, strconv.Itoa(productID), userID, nowStr, ttlStr).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "NOSCRIPT") {
			res, err = s.Client.Eval(ctx, defaultDecrStockLua, []string{productKey, OrderStreamKey}, reservationID, strconv.Itoa(productID), userID, nowStr, ttlStr).Result()
			if err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	if num, ok := res.(int64); ok {
		return num, nil
	}

	return 0, fmt.Errorf("unexpected script result type: %T (%v)", res, res)
}

// ReadStreamBatch reads up to `count` events from the stream starting after `lastID`.
func (s *RedisService) ReadStreamBatch(ctx context.Context, lastID string, count int64, block time.Duration) ([]ReservationEvent, string, error) {
	if lastID == "" {
		lastID = "0-0"
	}

	streams, err := s.Client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{OrderStreamKey, lastID},
		Count:   count,
		Block:   block,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return nil, lastID, nil // Timeout with no new messages
		}
		return nil, lastID, err
	}

	var events []ReservationEvent
	newLastID := lastID

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			newLastID = msg.ID

			event := ReservationEvent{
				StreamID: msg.ID,
			}

			if v, ok := msg.Values["reservation_id"].(string); ok {
				event.ReservationID = v
			}
			if v, ok := msg.Values["product_id"].(string); ok {
				event.ProductID, _ = strconv.Atoi(v)
			}
			if v, ok := msg.Values["user_id"].(string); ok {
				event.UserID = v
			}
			if v, ok := msg.Values["created_at"].(string); ok {
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					event.CreatedAt = t
				}
			}
			if event.CreatedAt.IsZero() {
				event.CreatedAt = time.Now().UTC()
			}

			events = append(events, event)
		}
	}

	return events, newLastID, nil
}

// ClearStream deletes the stream during reset
func (s *RedisService) ClearStream(ctx context.Context) error {
	return s.Client.Del(ctx, OrderStreamKey).Err()
}

// -----------------------------------------------------------------------------
// Phase 5: Idempotency & TTL / Rollback Methods
// -----------------------------------------------------------------------------

// TryAcquireIdempotencyLock attempts to set idempotency:{reservation_id} "PROCESSING" NX EX 60s.
// Returns true if the lock was acquired (first time processing this reservation).
// Returns false if the key already exists (duplicate delivery detected - at-least-once defense).
func (s *RedisService) TryAcquireIdempotencyLock(ctx context.Context, reservationID string, ttl time.Duration) (bool, error) {
	key := fmt.Sprintf("idempotency:%s", reservationID)
	return s.Client.SetNX(ctx, key, "PROCESSING", ttl).Result()
}

// MarkIdempotencyCompleted updates the idempotency key to "COMPLETED" while preserving TTL.
func (s *RedisService) MarkIdempotencyCompleted(ctx context.Context, reservationID string) error {
	key := fmt.Sprintf("idempotency:%s", reservationID)
	return s.Client.SetXX(ctx, key, "COMPLETED", redis.KeepTTL).Err()
}

// OrderTTLKey returns the Redis key for reservation TTL expiration tracking.
func OrderTTLKey(reservationID string) string {
	return fmt.Sprintf("order:%s:ttl", reservationID)
}

// GetReservationTTL returns the remaining TTL for a reservation.
func (s *RedisService) GetReservationTTL(ctx context.Context, reservationID string) (time.Duration, error) {
	key := OrderTTLKey(reservationID)
	return s.Client.TTL(ctx, key).Result()
}

// DeleteReservationTTL deletes the TTL key when payment is confirmed, preventing rollback.
func (s *RedisService) DeleteReservationTTL(ctx context.Context, reservationID string) error {
	key := OrderTTLKey(reservationID)
	return s.Client.Del(ctx, key).Err()
}

// IncrProductStock atomically increments product stock in Redis via INCR (used on rollback/timeout).
func (s *RedisService) IncrProductStock(ctx context.Context, productID int) (int64, error) {
	key := ProductStockKey(productID)
	return s.Client.Incr(ctx, key).Result()
}

// SubscribeKeyExpiration subscribes to Redis keyspace notifications for key expiration events (__keyevent@0__:expired).
func (s *RedisService) SubscribeKeyExpiration(ctx context.Context) *redis.PubSub {
	return s.Client.Subscribe(ctx, "__keyevent@0__:expired")
}

