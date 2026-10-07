package spatial

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"geopulse/internal/buffer"
	"geopulse/internal/ingestion"
)

// setupTestRedis initializes a Redis client connected to local test instance with isolated key namespaces.
func setupTestRedis(t *testing.T) (*GeoStore, *Sweeper, func()) {
	t.Helper()

	cfg := DefaultRedisConfig()
	// Unique test key namespaces to guarantee isolated execution
	prefix := fmt.Sprintf("test:%d:", time.Now().UnixNano())
	cfg.ActiveKey = prefix + "drivers:active"
	cfg.HeartbeatKey = prefix + "drivers:heartbeat"
	cfg.MetaKeyPrefix = prefix + "driver:meta:"
	cfg.PubSubChannelPrefix = prefix + "driver.updates."
	cfg.StaleThreshold = 2 * time.Second
	cfg.SweepInterval = 100 * time.Millisecond

	client, err := NewRedisClient(cfg, nil)
	if err != nil {
		t.Skipf("Redis instance not available at %s, skipping test: %v", cfg.Addr, err)
		return nil, nil, func() {}
	}

	geoStore := NewGeoStore(client, cfg, DefaultRegionResolver, nil)
	sweeper := NewSweeper(client, cfg, nil)

	cleanup := func() {
		ctx := context.Background()
		_ = client.Del(ctx, cfg.ActiveKey, cfg.HeartbeatKey).Err()
		_ = client.Close()
	}

	return geoStore, sweeper, cleanup
}

func TestRedisConnectionAndPool(t *testing.T) {
	cfg := DefaultRedisConfig()
	client, err := NewRedisClient(cfg, nil)
	if err != nil {
		t.Skipf("Skipping: Redis unavailable at %s: %v", cfg.Addr, err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pong, err := client.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if pong != "PONG" {
		t.Errorf("Expected PONG, got %s", pong)
	}
}

func TestGeoStore_UpdateAndSearch(t *testing.T) {
	geoStore, _, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UnixMilli()

	// 1. Insert known locations around Istanbul:
	// - Sultanahmet: 41.0082, 28.9784 (Query center)
	// - Taksim:      41.0370, 28.9850 (~3.2 km from Sultanahmet)
	// - Kadıköy:     40.9910, 29.0250 (~4.4 km from Sultanahmet)
	// - Ankara:      39.9208, 32.8541 (~350 km away)
	drivers := []*telemetryv1.LocationPing{
		{
			DriverId:  "driver-sultanahmet",
			Latitude:  41.0082,
			Longitude: 28.9784,
			Bearing:   90.0,
			Speed:     45.0,
			Timestamp: now,
		},
		{
			DriverId:  "driver-taksim",
			Latitude:  41.0370,
			Longitude: 28.9850,
			Bearing:   180.0,
			Speed:     60.0,
			Timestamp: now,
		},
		{
			DriverId:  "driver-kadikoy",
			Latitude:  40.9910,
			Longitude: 29.0250,
			Bearing:   270.0,
			Speed:     50.0,
			Timestamp: now,
		},
		{
			DriverId:  "driver-ankara",
			Latitude:  39.9208,
			Longitude: 32.8541,
			Bearing:   0.0,
			Speed:     90.0,
			Timestamp: now,
		},
	}

	for _, d := range drivers {
		if err := geoStore.UpdateDriverLocation(ctx, d); err != nil {
			t.Fatalf("Failed updating driver location: %v", err)
		}
	}

	// 2. Query radius of 3.5 km around Sultanahmet: should return Sultanahmet and Taksim, but NOT Kadıköy or Ankara
	query := &telemetryv1.RadiusQuery{
		Latitude:  41.0082,
		Longitude: 28.9784,
		RadiusKm:  3.5,
		Limit:     10,
	}

	res, err := geoStore.GetNearbyDrivers(ctx, query)
	if err != nil {
		t.Fatalf("GetNearbyDrivers failed: %v", err)
	}

	if len(res.Drivers) != 2 {
		t.Fatalf("Expected 2 drivers within 3.5 km, found %d", len(res.Drivers))
	}

	// Verify order is sorted ASC by distance
	first := res.Drivers[0]
	second := res.Drivers[1]

	if first.DriverId != "driver-sultanahmet" {
		t.Errorf("Expected first driver to be driver-sultanahmet, got %s", first.DriverId)
	}
	if first.DistanceKm > 0.01 {
		t.Errorf("Expected sultanahmet distance ~0 km, got %f", first.DistanceKm)
	}
	if math.Abs(float64(first.Bearing-90.0)) > 0.1 || math.Abs(float64(first.Speed-45.0)) > 0.1 {
		t.Errorf("Metadata mismatch for first driver: bearing=%f, speed=%f", first.Bearing, first.Speed)
	}

	if second.DriverId != "driver-taksim" {
		t.Errorf("Expected second driver to be driver-taksim, got %s", second.DriverId)
	}
	if second.DistanceKm < 3.0 || second.DistanceKm > 3.6 {
		t.Errorf("Expected taksim distance ~3.2 km, got %f", second.DistanceKm)
	}

	// 3. Expand radius to 5.0 km: Kadıköy should now also appear
	query.RadiusKm = 5.0
	resExpanded, err := geoStore.GetNearbyDrivers(ctx, query)
	if err != nil {
		t.Fatalf("GetNearbyDrivers 5km failed: %v", err)
	}
	if len(resExpanded.Drivers) != 3 {
		t.Errorf("Expected 3 drivers within 5.0 km, found %d", len(resExpanded.Drivers))
	}
}

func TestSweeper_GhostDriverEviction(t *testing.T) {
	geoStore, sweeper, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UnixMilli()

	// Insert 1 fresh active driver
	activeDriver := &telemetryv1.LocationPing{
		DriverId:  "driver-active-live",
		Latitude:  41.0082,
		Longitude: 28.9784,
		Timestamp: now,
	}
	if err := geoStore.UpdateDriverLocation(ctx, activeDriver); err != nil {
		t.Fatalf("Failed adding active driver: %v", err)
	}

	// Insert 1 stale ghost driver with timestamp 10 seconds in the past
	staleDriver := &telemetryv1.LocationPing{
		DriverId:  "driver-ghost-stale",
		Latitude:  41.0100,
		Longitude: 28.9800,
		Timestamp: now - 10000, // 10s ago (threshold is 2s)
	}
	if err := geoStore.UpdateDriverLocation(ctx, staleDriver); err != nil {
		t.Fatalf("Failed adding stale driver: %v", err)
	}

	// Run single eviction sweep
	prunedCount, err := sweeper.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce failed: %v", err)
	}
	if prunedCount != 1 {
		t.Fatalf("Expected 1 driver pruned, got %d", prunedCount)
	}

	// Verify that querying around Sultanahmet only returns the active driver
	list, err := geoStore.GetNearbyDrivers(ctx, &telemetryv1.RadiusQuery{
		Latitude:  41.0082,
		Longitude: 28.9784,
		RadiusKm:  5.0,
	})
	if err != nil {
		t.Fatalf("GetNearbyDrivers failed: %v", err)
	}

	if len(list.Drivers) != 1 {
		t.Fatalf("Expected only 1 driver after sweep, got %d", len(list.Drivers))
	}
	if list.Drivers[0].DriverId != "driver-active-live" {
		t.Errorf("Expected driver-active-live, got %s", list.Drivers[0].DriverId)
	}
}

func TestPubSub_FanOut(t *testing.T) {
	geoStore, _, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ping := &telemetryv1.LocationPing{
		DriverId:  "driver-pubsub-test",
		Latitude:  41.0082,
		Longitude: 28.9784,
		Bearing:   45.0,
		Speed:     55.5,
		Timestamp: time.Now().UnixMilli(),
	}

	regionID := DefaultRegionResolver(ping.Latitude, ping.Longitude)

	// Subscribe to the regional fan-out channel
	subCh, unsubscribe, err := geoStore.SubscribeRegion(ctx, regionID)
	if err != nil {
		t.Fatalf("Failed subscribing to region %s: %v", regionID, err)
	}
	defer unsubscribe()

	// Allow subscriber registration to stabilize
	time.Sleep(50 * time.Millisecond)

	// Publish ping
	if err := geoStore.PublishPing(ctx, ping); err != nil {
		t.Fatalf("Failed publishing ping: %v", err)
	}

	// Wait for published ping on subscriber channel
	select {
	case receivedPing, ok := <-subCh:
		if !ok {
			t.Fatal("Subscriber channel closed unexpectedly")
		}
		if receivedPing.DriverId != ping.DriverId {
			t.Errorf("Expected driver %s, got %s", ping.DriverId, receivedPing.DriverId)
		}
		if math.Abs(receivedPing.Latitude-ping.Latitude) > 1e-6 {
			t.Errorf("Latitude mismatch: %f != %f", receivedPing.Latitude, ping.Latitude)
		}
		if math.Abs(float64(receivedPing.Bearing-ping.Bearing)) > 1e-4 {
			t.Errorf("Bearing mismatch: %f != %f", receivedPing.Bearing, ping.Bearing)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for pubsub broadcast message")
	}
}

func TestEndToEnd_IngestionWithRedis(t *testing.T) {
	geoStore, _, cleanup := setupTestRedis(t)
	defer cleanup()

	server := ingestion.NewTelemetryServer(ingestion.Config{
		BufferCapacity: 100,
		DropStrategy:   buffer.DropOldest,
		NumWorkers:     2,
	}, nil)

	// Connect GeoStore hook to worker pool and query engine
	server.WorkerPool().RegisterHook(geoStore.ProcessPing)
	server.SetSpatialQuerier(geoStore)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server.Start(ctx)
	defer server.Stop()

	// Push 5 mock pings directly into server buffer to simulate ingested stream
	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		ping := &telemetryv1.LocationPing{
			DriverId:  fmt.Sprintf("driver-fleet-%d", i),
			Latitude:  41.0082 + float64(i)*0.001,
			Longitude: 28.9784 + float64(i)*0.001,
			Bearing:   float32(i * 45),
			Speed:     50.0,
			Timestamp: now,
		}
		server.Buffer().Push(ping)
	}

	// Give workers time to dequeue and process
	time.Sleep(200 * time.Millisecond)

	// Query nearby drivers via gRPC server API
	query := &telemetryv1.RadiusQuery{
		Latitude:  41.0082,
		Longitude: 28.9784,
		RadiusKm:  2.0,
		Limit:     10,
	}

	driverList, err := server.GetNearbyDrivers(ctx, query)
	if err != nil {
		t.Fatalf("GetNearbyDrivers failed: %v", err)
	}

	if len(driverList.Drivers) != 5 {
		t.Fatalf("Expected 5 drivers found in Redis via gRPC query, got %d", len(driverList.Drivers))
	}
}
