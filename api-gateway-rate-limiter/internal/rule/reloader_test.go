package rule

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var configV1 = `
global:
  capacity: 100
  rate: 10.0
  window: 60s
`

var configV2 = `
global:
  capacity: 500
  rate: 50.0
  window: 60s
`

var invalidConfig = `
global:
  capacity: -10
  window: 60s
`

func TestReloader_AtomicSwapConcurrency(t *testing.T) {
	reloader, err := NewReloader([]byte(configV1), ReloaderConfig{})
	if err != nil {
		t.Fatalf("failed to create reloader: %v", err)
	}
	defer reloader.Close()

	var wg sync.WaitGroup
	var stopFlag atomic.Bool
	var seenV1Count, seenV2Count atomic.Int64

	req := &MatchRequest{Path: "/test", Method: "GET"}

	// 20 concurrent reader goroutines continuously reading engine
	readers := 20
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopFlag.Load() {
				engine := reloader.Engine()
				matched := engine.Match(req)
				if matched.Capacity == 100 {
					seenV1Count.Add(1)
				} else if matched.Capacity == 500 {
					seenV2Count.Add(1)
				}
			}
		}()
	}

	// Let readers spin on V1
	time.Sleep(30 * time.Millisecond)

	// Execute atomic pointer swap to V2
	err = reloader.ReloadFromBytes([]byte(configV2))
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}

	// Let readers spin on V2
	time.Sleep(30 * time.Millisecond)
	stopFlag.Store(true)
	wg.Wait()

	if seenV1Count.Load() == 0 {
		t.Error("expected readers to have read V1")
	}
	if seenV2Count.Load() == 0 {
		t.Error("expected readers to have read V2 after atomic swap")
	}
	if reloader.ReloadCount() != 1 {
		t.Errorf("expected reload count 1, got %d", reloader.ReloadCount())
	}
}

func TestReloader_InvalidConfigRejection(t *testing.T) {
	var errorReported error
	reloader, err := NewReloader([]byte(configV1), ReloaderConfig{
		OnReloadError: func(err error) {
			errorReported = err
		},
	})
	if err != nil {
		t.Fatalf("failed to create reloader: %v", err)
	}
	defer reloader.Close()

	// Attempt reload with invalid config
	err = reloader.ReloadFromBytes([]byte(invalidConfig))
	if err == nil {
		t.Fatal("expected reload error for invalid config, got nil")
	}

	if errorReported == nil {
		t.Fatal("expected OnReloadError callback to be invoked")
	}

	// Verify previous configuration (V1: 100) is preserved without downtime
	req := &MatchRequest{Path: "/test", Method: "GET"}
	matched := reloader.Engine().Match(req)
	if matched.Capacity != 100 {
		t.Errorf("expected original capacity 100 to remain active, got %d", matched.Capacity)
	}
}

func TestReloader_FileWatcher(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "rules.yaml")

	err := os.WriteFile(configPath, []byte(configV1), 0644)
	if err != nil {
		t.Fatalf("failed to write initial config: %v", err)
	}

	reloader, err := NewReloader([]byte(configV1), ReloaderConfig{
		ConfigFilePath: configPath,
		WatchInterval:  30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create reloader: %v", err)
	}
	defer reloader.Close()

	time.Sleep(50 * time.Millisecond)

	// Update file on disk
	err = os.WriteFile(configPath, []byte(configV2), 0644)
	if err != nil {
		t.Fatalf("failed to update config file: %v", err)
	}

	// Wait for watcher to trigger reload
	deadline := time.Now().Add(1 * time.Second)
	updated := false
	for time.Now().Before(deadline) {
		if reloader.ReloadCount() >= 1 {
			req := &MatchRequest{Path: "/test", Method: "GET"}
			if reloader.Engine().Match(req).Capacity == 500 {
				updated = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !updated {
		t.Fatal("file watcher failed to reload updated config within deadline")
	}
}

func TestReloader_SIGHUP(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "rules_sighup.yaml")

	err := os.WriteFile(configPath, []byte(configV1), 0644)
	if err != nil {
		t.Fatalf("failed to write initial config: %v", err)
	}

	reloader, err := NewReloader([]byte(configV1), ReloaderConfig{
		ConfigFilePath: configPath,
		EnableSIGHUP:   true,
	})
	if err != nil {
		t.Fatalf("failed to create reloader: %v", err)
	}
	defer reloader.Close()

	// Update file on disk
	err = os.WriteFile(configPath, []byte(configV2), 0644)
	if err != nil {
		t.Fatalf("failed to update file: %v", err)
	}

	// Send SIGHUP to self
	err = syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	if err != nil {
		t.Fatalf("failed to send SIGHUP: %v", err)
	}

	// Wait for SIGHUP reload
	deadline := time.Now().Add(1 * time.Second)
	updated := false
	for time.Now().Before(deadline) {
		if reloader.ReloadCount() >= 1 {
			req := &MatchRequest{Path: "/test", Method: "GET"}
			if reloader.Engine().Match(req).Capacity == 500 {
				updated = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !updated {
		t.Fatal("SIGHUP failed to trigger reload within deadline")
	}
}

func startEphemeralRedis(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	cmd := exec.Command("redis-server", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Skipf("redis-server unavailable: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	return addr, cleanup
}

func TestReloader_RedisPubSub(t *testing.T) {
	addr, cleanup := startEphemeralRedis(t)
	defer cleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	reloader, err := NewReloader([]byte(configV1), ReloaderConfig{
		RedisClient:     rdb,
		RedisReloadChan: "gateway:reload:test",
	})
	if err != nil {
		t.Fatalf("failed to create reloader: %v", err)
	}
	defer reloader.Close()

	time.Sleep(100 * time.Millisecond) // Allow subscription to be active

	// Publish new config payload across distributed nodes
	err = rdb.Publish(context.Background(), "gateway:reload:test", configV2).Err()
	if err != nil {
		t.Fatalf("failed to publish reload message: %v", err)
	}

	// Wait for PubSub reload
	deadline := time.Now().Add(1 * time.Second)
	updated := false
	for time.Now().Before(deadline) {
		if reloader.ReloadCount() >= 1 {
			req := &MatchRequest{Path: "/test", Method: "GET"}
			if reloader.Engine().Match(req).Capacity == 500 {
				updated = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !updated {
		t.Fatal("Redis PubSub failed to reload config within deadline")
	}
}
