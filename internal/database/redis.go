package database

import (
	"context"
	"fmt"
	"log"
	"os"

	"flashsale/internal/config"
	"github.com/redis/go-redis/v9"
)

const defaultDecrStockLua = 

`local stock = redis.call('GET', KEYS[1])
if not stock then
    return -1 -- Ürün bulunamadı
end
if tonumber(stock) <= 0 then
    return 0 -- Stok bitti
end
redis.call('DECR', KEYS[1])
return 1 -- Başarılı
`

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

// DecrStock executes the atomic Lua script via EVALSHA
// Returns:
//
//	 1: Stock successfully decremented
//	 0: Out of stock (stock <= 0)
//	-1: Product key not found in Redis
func (s *RedisService) DecrStock(ctx context.Context, productID int) (int64, error) {
	key := ProductStockKey(productID)

	res, err := s.Client.EvalSha(ctx, s.decrStockSHA, []string{key}).Result()
	if err != nil {
		// If NOSCRIPT error occurs, fallback to EVAL
		if redis.HasErrorPrefix(err, "NOSCRIPT") {
			res, err = s.Client.Eval(ctx, defaultDecrStockLua, []string{key}).Result()
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
