package concurrency_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/concurrency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

func TestOrderAccountIDs_DeadlockPrevention(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "Karışık ve tekrarlı ID'ler sıralanmalı ve tekilleştirilmeli",
			input:    []string{"acc-99", "acc-01", "acc-50", "acc-01", "acc-10"},
			expected: []string{"acc-01", "acc-10", "acc-50", "acc-99"},
		},
		{
			name:     "A-B ve B-A transferlerinde aynı kilit sırası üretilmeli",
			input:    []string{"account-B", "account-A"},
			expected: []string{"account-A", "account-B"},
		},
		{
			name:     "Boş dilim",
			input:    []string{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := concurrency.OrderAccountIDs(tt.input)
			if len(res) != len(tt.expected) {
				t.Fatalf("uzunluk uyuşmuyor: beklenen %d, alınan %d", len(tt.expected), len(res))
			}
			for i := range res {
				if res[i] != tt.expected[i] {
					t.Errorf("[%d] eleman uyuşmuyor: beklenen %s, alınan %s", i, tt.expected[i], res[i])
				}
			}
		})
	}
}

func TestExecuteWithRetry_SuccessOnRetry(t *testing.T) {
	ctx := context.Background()
	cfg := concurrency.RetryConfig{
		MaxRetries: 3,
		BaseDelay:  2 * time.Millisecond,
		MaxDelay:   20 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     false,
	}

	attempts := 0
	err := concurrency.ExecuteWithRetry(ctx, cfg, func(attempt int) error {
		attempts++
		if attempt < 2 {
			return domain.ErrOptimisticLockConflict // İlk 2 denemede çakışma simülasyonu
		}
		return nil // 3. denemede başarı
	})

	if err != nil {
		t.Fatalf("işlem başarılı olmalıydı, alınan hata: %v", err)
	}
	if attempts != 3 {
		t.Errorf("beklenen deneme sayısı 3, gerçekleşen: %d", attempts)
	}
}

func TestExecuteWithRetry_NonRetryableError(t *testing.T) {
	ctx := context.Background()
	cfg := concurrency.DefaultRetryConfig()

	fatalErr := errors.New("kritik iş hatası")
	attempts := 0
	err := concurrency.ExecuteWithRetry(ctx, cfg, func(attempt int) error {
		attempts++
		return fatalErr
	})

	if !errors.Is(err, fatalErr) {
		t.Fatalf("beklenen fatal hata dönmedi: %v", err)
	}
	if attempts != 1 {
		t.Errorf("tekrar denenmemeliydi, deneme sayısı: %d", attempts)
	}
}

func TestExecuteWithRetry_ExceedsMaxRetries(t *testing.T) {
	ctx := context.Background()
	cfg := concurrency.RetryConfig{
		MaxRetries: 2,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   10 * time.Millisecond,
		Multiplier: 1.5,
	}

	attempts := 0
	err := concurrency.ExecuteWithRetry(ctx, cfg, func(attempt int) error {
		attempts++
		return domain.ErrOptimisticLockConflict
	})

	if err == nil {
		t.Fatal("hata bekleniyordu")
	}
	if !errors.Is(err, domain.ErrOptimisticLockConflict) {
		t.Errorf("ErrOptimisticLockConflict sarmalanmış olmalıydı: %v", err)
	}
	if attempts != 3 { // 0, 1, 2 = 3 deneme
		t.Errorf("toplam 3 deneme yapılmalıydı, yapılan: %d", attempts)
	}
}
