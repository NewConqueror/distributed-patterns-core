package rule

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReloaderConfig defines options for dynamic rule management and hot-reload.
type ReloaderConfig struct {
	ConfigFilePath    string                // Path to YAML/JSON rules file
	WatchInterval     time.Duration         // Polling interval for file change detection (e.g. 1s)
	EnableSIGHUP      bool                  // Listen for syscall.SIGHUP to reload
	RedisClient       redis.UniversalClient // Optional: for distributed Redis Pub/Sub reloads
	RedisReloadChan   string                // Redis Pub/Sub channel name (default: "gateway:reload")
	OnReloadSuccess   func(newEngine *Engine)
	OnReloadError     func(err error)
}

// Reloader manages zero-downtime hot-reloading of rate limit rules using atomic pointer swaps.
type Reloader struct {
	cfg           ReloaderConfig
	currentEngine atomic.Pointer[Engine]
	reloadCount   atomic.Uint64
	lastModTime   time.Time
	mu            sync.Mutex

	stopCh  chan struct{}
	closeWg sync.WaitGroup
}

// NewReloader creates a new Reloader with the initial configuration.
func NewReloader(initialData []byte, cfg ReloaderConfig) (*Reloader, error) {
	initialCfg, err := ParseConfig(initialData)
	if err != nil {
		return nil, fmt.Errorf("initial configuration invalid: %w", err)
	}

	engine := NewEngine(initialCfg)

	r := &Reloader{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
	r.currentEngine.Store(engine)

	if cfg.ConfigFilePath != "" {
		if fi, err := os.Stat(cfg.ConfigFilePath); err == nil {
			r.lastModTime = fi.ModTime()
		}
	}

	// 1. SIGHUP Signal Listener
	if cfg.EnableSIGHUP && cfg.ConfigFilePath != "" {
		r.closeWg.Add(1)
		go r.listenSignals()
	}

	// 2. File Watcher Loop
	if cfg.WatchInterval > 0 && cfg.ConfigFilePath != "" {
		r.closeWg.Add(1)
		go r.watchFile()
	}

	// 3. Distributed Redis Pub/Sub Listener
	if cfg.RedisClient != nil {
		if cfg.RedisReloadChan == "" {
			cfg.RedisReloadChan = "gateway:reload"
		}
		r.cfg.RedisReloadChan = cfg.RedisReloadChan
		r.closeWg.Add(1)
		go r.listenRedisPubSub()
	}

	return r, nil
}

// Engine returns the currently active Engine via a lock-free atomic pointer load.
func (r *Reloader) Engine() *Engine {
	return r.currentEngine.Load()
}

// ReloadFromFile reads the config file from disk and atomically swaps the rule engine.
// If the new file is corrupted or invalid, the previous engine is preserved without interruption.
func (r *Reloader) ReloadFromFile(filePath string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		r.reportError(fmt.Errorf("failed to read config file %q: %w", filePath, err))
		return err
	}

	return r.reloadLocked(data)
}

// ReloadFromBytes parses raw configuration bytes and atomically swaps the rule engine.
func (r *Reloader) ReloadFromBytes(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.reloadLocked(data)
}

func (r *Reloader) reloadLocked(data []byte) error {
	newCfg, err := ParseConfig(data)
	if err != nil {
		r.reportError(fmt.Errorf("reload rejected: %w", err))
		return err
	}

	newEngine := NewEngine(newCfg)
	r.currentEngine.Store(newEngine)
	r.reloadCount.Add(1)

	if r.cfg.OnReloadSuccess != nil {
		r.cfg.OnReloadSuccess(newEngine)
	}

	return nil
}

func (r *Reloader) reportError(err error) {
	if r.cfg.OnReloadError != nil {
		r.cfg.OnReloadError(err)
	}
}

// ReloadCount returns the total number of successful hot-reloads executed.
func (r *Reloader) ReloadCount() uint64 {
	return r.reloadCount.Load()
}

// listenSignals handles OS SIGHUP signals.
func (r *Reloader) listenSignals() {
	defer r.closeWg.Done()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)
	defer signal.Stop(sigCh)

	for {
		select {
		case <-r.stopCh:
			return
		case <-sigCh:
			_ = r.ReloadFromFile(r.cfg.ConfigFilePath)
		}
	}
}

// watchFile checks file modification timestamps periodically.
func (r *Reloader) watchFile() {
	defer r.closeWg.Done()

	ticker := time.NewTicker(r.cfg.WatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			fi, err := os.Stat(r.cfg.ConfigFilePath)
			if err != nil {
				continue
			}

			r.mu.Lock()
			if fi.ModTime().After(r.lastModTime) {
				r.lastModTime = fi.ModTime()
				r.mu.Unlock()
				_ = r.ReloadFromFile(r.cfg.ConfigFilePath)
			} else {
				r.mu.Unlock()
			}
		}
	}
}

// listenRedisPubSub listens for distributed reload broadcasts across gateway nodes.
func (r *Reloader) listenRedisPubSub() {
	defer r.closeWg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-r.stopCh
		cancel()
	}()

	pubsub := r.cfg.RedisClient.Subscribe(ctx, r.cfg.RedisReloadChan)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-r.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if msg == nil {
				continue
			}

			// If message payload contains full config (starts with '{' or 'global:')
			payload := strings.TrimSpace(msg.Payload)
			if len(payload) > 0 && (strings.HasPrefix(payload, "{") || strings.HasPrefix(payload, "global:")) {
				_ = r.ReloadFromBytes([]byte(payload))
			} else if r.cfg.ConfigFilePath != "" {
				// Otherwise reload from configured file
				_ = r.ReloadFromFile(r.cfg.ConfigFilePath)
			}
		}
	}
}

// Close gracefully shuts down all hot-reload listeners.
func (r *Reloader) Close() error {
	select {
	case <-r.stopCh:
		return errors.New("already closed")
	default:
		close(r.stopCh)
	}

	r.closeWg.Wait()
	return nil
}
