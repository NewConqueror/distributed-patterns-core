package ingestion

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"geopulse/internal/buffer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupTestServer(t *testing.T, cfg Config) (*TelemetryServer, *grpc.ClientConn, func()) {
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()

	telemetryServer := NewTelemetryServer(cfg, nil)
	telemetryv1.RegisterTelemetryServiceServer(grpcServer, telemetryServer)

	ctx, cancel := context.WithCancel(context.Background())
	telemetryServer.Start(ctx)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("Server error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.GracefulStop()
		telemetryServer.Stop()
		cancel()
		lis.Close()
	}

	return telemetryServer, conn, cleanup
}

func TestTelemetryServer_StreamLocation_Success(t *testing.T) {
	cfg := Config{
		BufferCapacity: 1000,
		DropStrategy:   buffer.DropOldest,
		NumWorkers:     2,
	}
	server, conn, cleanup := setupTestServer(t, cfg)
	defer cleanup()

	client := telemetryv1.NewTelemetryServiceClient(conn)
	stream, err := client.StreamLocation(context.Background())
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	var hookProcessed atomic.Int64
	server.WorkerPool().RegisterHook(func(ctx context.Context, ping *telemetryv1.LocationPing) error {
		hookProcessed.Add(1)
		return nil
	})

	const numPings = 50
	for i := 0; i < numPings; i++ {
		err := stream.Send(&telemetryv1.LocationPing{
			DriverId:  "driver-test-1",
			Latitude:  41.0082,
			Longitude: 28.9784,
			Bearing:   180.0,
			Speed:     50.0,
			Timestamp: time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("Send error at index %d: %v", i, err)
		}
	}

	ack, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv failed: %v", err)
	}

	if !ack.Success {
		t.Fatalf("Expected success true, got false: %s", ack.Message)
	}
	if ack.ProcessedCount != numPings {
		t.Fatalf("Expected processed count %d, got %d", numPings, ack.ProcessedCount)
	}

	// Wait for worker pool to drain buffer
	time.Sleep(100 * time.Millisecond)

	if hookProcessed.Load() != numPings {
		t.Fatalf("Expected worker hook to process %d pings, got %d", numPings, hookProcessed.Load())
	}
}

func TestTelemetryServer_StreamLocation_ClientDisconnectCleanup(t *testing.T) {
	cfg := DefaultConfig()
	server, conn, cleanup := setupTestServer(t, cfg)
	defer cleanup()

	client := telemetryv1.NewTelemetryServiceClient(conn)

	// Open stream with cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.StreamLocation(ctx)
	if err != nil {
		t.Fatalf("Stream open error: %v", err)
	}

	err = stream.Send(&telemetryv1.LocationPing{
		DriverId: "driver-disconnect-test",
		Latitude: 41.0,
	})
	if err != nil {
		t.Fatalf("Send error: %v", err)
	}

	// Check active stream gauge
	time.Sleep(50 * time.Millisecond)
	if server.Metrics().ActiveStreams.Load() != 1 {
		t.Fatalf("Expected 1 active stream, got %d", server.Metrics().ActiveStreams.Load())
	}

	// Cancel context abruptly (simulate disconnect)
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Verify active stream count drops to 0
	if server.Metrics().ActiveStreams.Load() != 0 {
		t.Fatalf("Expected 0 active streams after disconnect, got %d", server.Metrics().ActiveStreams.Load())
	}
}

func TestTelemetryServer_StreamLocation_BackpressureDrop(t *testing.T) {
	// Tiny buffer capacity: 5
	cfg := Config{
		BufferCapacity: 5,
		DropStrategy:   buffer.DropOldest,
		NumWorkers:     0, // No workers to consume, so buffer fills immediately
	}
	server, conn, cleanup := setupTestServer(t, cfg)
	defer cleanup()

	client := telemetryv1.NewTelemetryServiceClient(conn)
	stream, err := client.StreamLocation(context.Background())
	if err != nil {
		t.Fatalf("Stream open error: %v", err)
	}

	const sendPings = 20
	for i := 0; i < sendPings; i++ {
		_ = stream.Send(&telemetryv1.LocationPing{
			DriverId: "driver-backpressure",
			Latitude: 41.0,
		})
	}

	ack, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		t.Fatalf("CloseAndRecv error: %v", err)
	}
	if ack.ProcessedCount != sendPings {
		t.Fatalf("Expected received count %d, got %d", sendPings, ack.ProcessedCount)
	}

	stats := server.Buffer().Stats()
	if stats.TotalDropped == 0 {
		t.Fatalf("Expected drops under backpressure, got 0 drops")
	}
	t.Logf("Backpressure Test: Total Enqueued: %d, Dropped: %d, Depth: %d",
		stats.TotalEnqueued, stats.TotalDropped, stats.CurrentDepth)
}
