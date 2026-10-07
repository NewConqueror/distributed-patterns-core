package spatial

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"geopulse/internal/metrics"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// GeoStore manages spatial indexing, radius queries, and real-time distribution
// using Redis Geospatial data structures and Pub/Sub.
type GeoStore struct {
	client         *redis.Client
	cfg            RedisConfig
	logger         *slog.Logger
	regionResolver RegionResolver
}

// NewGeoStore initializes a GeoStore instance.
func NewGeoStore(client *redis.Client, cfg RedisConfig, resolver RegionResolver, logger *slog.Logger) *GeoStore {
	if logger == nil {
		logger = slog.Default()
	}
	if resolver == nil {
		resolver = DefaultRegionResolver
	}

	return &GeoStore{
		client:         client,
		cfg:            cfg,
		logger:         logger,
		regionResolver: resolver,
	}
}

// Client returns the underlying redis client.
func (s *GeoStore) Client() *redis.Client {
	return s.client
}

// Config returns the active RedisConfig.
func (s *GeoStore) Config() RedisConfig {
	return s.cfg
}

// ProcessPing implements the ingestion.ProcessHook signature. It updates the
// Redis spatial index and immediately broadcasts the ping to the regional pub/sub channel.
func (s *GeoStore) ProcessPing(ctx context.Context, ping *telemetryv1.LocationPing) error {
	start := time.Now()

	if err := s.UpdateDriverLocation(ctx, ping); err != nil {
		return fmt.Errorf("spatial update failed: %w", err)
	}

	if err := s.PublishPing(ctx, ping); err != nil {
		return fmt.Errorf("pubsub fan-out failed: %w", err)
	}

	metrics.Default().RedisLatency.Observe(time.Since(start).Seconds())
	return nil
}

// UpdateDriverLocation performs atomic spatial indexing in Redis:
// 1. GEOADD drivers:active <lon> <lat> <driver_id>
// 2. ZADD drivers:heartbeat <timestamp> <driver_id>
// 3. HSET driver:meta:<driver_id> bearing, speed, timestamp with TTL
func (s *GeoStore) UpdateDriverLocation(ctx context.Context, ping *telemetryv1.LocationPing) error {
	pipe := s.client.Pipeline()

	// 1. In-memory Geospatial Sorted Set
	pipe.GeoAdd(ctx, s.cfg.ActiveKey, &redis.GeoLocation{
		Name:      ping.DriverId,
		Longitude: ping.Longitude,
		Latitude:  ping.Latitude,
	})

	// 2. Heartbeat tracking Sorted Set (Score = timestamp in ms)
	pipe.ZAdd(ctx, s.cfg.HeartbeatKey, redis.Z{
		Score:  float64(ping.Timestamp),
		Member: ping.DriverId,
	})

	// 3. Driver telemetry metadata cache
	metaKey := s.cfg.MetaKeyPrefix + ping.DriverId
	pipe.HSet(ctx, metaKey, map[string]interface{}{
		"bearing":   ping.Bearing,
		"speed":     ping.Speed,
		"timestamp": ping.Timestamp,
	})
	pipe.Expire(ctx, metaKey, s.cfg.StaleThreshold*2)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed executing spatial pipeline for driver %s: %w", ping.DriverId, err)
	}

	return nil
}

// GetNearbyDrivers executes a radius search using Redis GEOSEARCH,
// retrieving matching driver IDs, coordinates, distances, and latest telemetry metadata.
func (s *GeoStore) GetNearbyDrivers(ctx context.Context, req *telemetryv1.RadiusQuery) (*telemetryv1.DriverList, error) {
	// Defensive parameter validation
	if req.Latitude < -90.0 || req.Latitude > 90.0 ||
		req.Longitude < -180.0 || req.Longitude > 180.0 {
		return nil, fmt.Errorf("invalid coordinates (lat: %f, lon: %f)", req.Latitude, req.Longitude)
	}
	if req.RadiusKm <= 0 {
		return nil, fmt.Errorf("invalid search radius: %f km (must be > 0)", req.RadiusKm)
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 50
	} else if limit > 500 {
		limit = 500
	}

	searchQuery := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  req.Longitude,
			Latitude:   req.Latitude,
			Radius:     req.RadiusKm,
			RadiusUnit: "km",
			Sort:       "ASC",
			Count:      limit,
		},
		WithCoord: true,
		WithDist:  true,
	}

	locations, err := s.client.GeoSearchLocation(ctx, s.cfg.ActiveKey, searchQuery).Result()
	if err != nil {
		return nil, fmt.Errorf("geosearch failed: %w", err)
	}

	nowMs := time.Now().UnixMilli()
	if len(locations) == 0 {
		return &telemetryv1.DriverList{
			Drivers:         []*telemetryv1.NearbyDriver{},
			SearchTimestamp: nowMs,
		}, nil
	}

	// Pipelined metadata fetch for found drivers
	pipe := s.client.Pipeline()
	metaCmds := make([]*redis.MapStringStringCmd, len(locations))
	for i, loc := range locations {
		metaCmds[i] = pipe.HGetAll(ctx, s.cfg.MetaKeyPrefix+loc.Name)
	}
	_, _ = pipe.Exec(ctx) // Non-blocking; if metadata expired or missing, fallback to defaults

	drivers := make([]*telemetryv1.NearbyDriver, 0, len(locations))
	for i, loc := range locations {
		var (
			bearing     float32 = 0
			speed       float32 = 0
			lastUpdated int64   = nowMs
		)

		if meta, err := metaCmds[i].Result(); err == nil && len(meta) > 0 {
			if b, err := strconv.ParseFloat(meta["bearing"], 32); err == nil {
				bearing = float32(b)
			}
			if sp, err := strconv.ParseFloat(meta["speed"], 32); err == nil {
				speed = float32(sp)
			}
			if ts, err := strconv.ParseInt(meta["timestamp"], 10, 64); err == nil {
				lastUpdated = ts
			}
		}

		drivers = append(drivers, &telemetryv1.NearbyDriver{
			DriverId:    loc.Name,
			Latitude:    loc.Latitude,
			Longitude:   loc.Longitude,
			Bearing:     bearing,
			Speed:       speed,
			DistanceKm:  loc.Dist,
			LastUpdated: lastUpdated,
		})
	}

	return &telemetryv1.DriverList{
		Drivers:         drivers,
		SearchTimestamp: nowMs,
	}, nil
}

// PublishPing broadcasts the telemetry update via Redis Pub/Sub to the channel
// corresponding to the driver's regional partition (driver.updates.<region_id>).
func (s *GeoStore) PublishPing(ctx context.Context, ping *telemetryv1.LocationPing) error {
	regionID := s.regionResolver(ping.Latitude, ping.Longitude)
	channel := s.cfg.PubSubChannelPrefix + regionID

	payload, err := proto.Marshal(ping)
	if err != nil {
		return fmt.Errorf("failed marshaling ping for pubsub: %w", err)
	}

	if err := s.client.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("failed publishing ping to channel %s: %w", channel, err)
	}

	return nil
}

// SubscribeRegion subscribes to real-time driver updates for a given region.
// It returns a channel receiving LocationPing messages and an unsubscribe cancel function.
func (s *GeoStore) SubscribeRegion(ctx context.Context, regionID string) (<-chan *telemetryv1.LocationPing, func(), error) {
	channel := s.cfg.PubSubChannelPrefix + regionID
	pubsub := s.client.Subscribe(ctx, channel)

	// Wait for subscription confirmation
	_, err := pubsub.Receive(ctx)
	if err != nil {
		_ = pubsub.Close()
		return nil, nil, fmt.Errorf("failed to subscribe to %s: %w", channel, err)
	}

	outCh := make(chan *telemetryv1.LocationPing, 100)
	subCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(outCh)
		defer pubsub.Close()

		msgCh := pubsub.Channel()
		for {
			select {
			case <-subCtx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var ping telemetryv1.LocationPing
				if err := proto.Unmarshal([]byte(msg.Payload), &ping); err == nil {
					select {
					case outCh <- &ping:
					case <-subCtx.Done():
						return
					}
				}
			}
		}
	}()

	unsubscribe := func() {
		cancel()
	}

	return outCh, unsubscribe, nil
}
