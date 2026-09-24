package concurrency

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

// RetryConfig, Optimistic Concurrency Control (OCC) çakışmalarında yeniden deneme yapılandırmasıdır.
type RetryConfig struct {
	MaxRetries int           // En fazla yeniden deneme sayısı
	BaseDelay  time.Duration // Başlangıç bekleme süresi (örn. 10ms)
	MaxDelay   time.Duration // Tavan bekleme süresi (örn. 500ms)
	Multiplier float64       // Katlanma katsayısı (örn. 2.0)
	Jitter     bool          // Thundering herd önlemek için rastgele sapma eklensin mi
}

// DefaultRetryConfig, OCC için standart exponential backoff yapılandırması
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 5,
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   500 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     true,
	}
}

// CalculateBackoff, deneme adımına (attempt, 0-indexed) göre bekleme süresini hesaplar.
func (c RetryConfig) CalculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return c.BaseDelay
	}

	mult := math.Pow(c.Multiplier, float64(attempt))
	delay := float64(c.BaseDelay) * mult

	if delay > float64(c.MaxDelay) {
		delay = float64(c.MaxDelay)
	}

	d := time.Duration(delay)

	if c.Jitter && d > 0 {
		// %20 oranında artı/eksi rastgele jitter ekle
		jitterRange := int64(d) / 5
		if jitterRange > 0 {
			randomJitter := rand.Int63n(jitterRange*2) - jitterRange
			d = time.Duration(int64(d) + randomJitter)
		}
	}

	if d < 0 {
		d = c.BaseDelay
	}
	return d
}

// ExecuteWithRetry, verilen işlemi OCC çakışması (ErrOptimisticLockConflict) durumunda
// üst katmanda Exponential Backoff ile yeniden dener.
func ExecuteWithRetry(ctx context.Context, cfg RetryConfig, fn func(attempt int) error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("işlem iptal edildi (context cancelled): %w", ctx.Err())
		default:
		}

		err := fn(attempt)
		if err == nil {
			return nil
		}

		lastErr = err

		// Yalnızca Optimistic Lock Conflict hatalarında yeniden dene
		if !errors.Is(err, domain.ErrOptimisticLockConflict) {
			return err
		}

		if attempt == cfg.MaxRetries {
			break
		}

		backoff := cfg.CalculateBackoff(attempt)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return fmt.Errorf("maksimum yeniden deneme sayısı (%d) aşıldı: %w", cfg.MaxRetries, lastErr)
}
