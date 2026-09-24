package domain_test

import (
	"math"
	"testing"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

func TestCurrency_CreationAndValidation(t *testing.T) {
	tests := []struct {
		name      string
		code      string
		decimals  int
		expectErr bool
	}{
		{"Geçerli USD", "USD", 2, false},
		{"Geçerli JPY (0 ondalık)", "JPY", 0, false},
		{"Geçerli KWD (3 ondalık)", "KWD", 3, false},
		{"Küçük harf reddedilmeli", "usd", 2, true},
		{"2 karakter reddedilmeli", "US", 2, true},
		{"4 karakter reddedilmeli", "USDD", 2, true},
		{"Rakam içeren kod reddedilmeli", "U12", 2, true},
		{"Negatif ondalık reddedilmeli", "EUR", -1, true},
		{"Aşırı yüksek ondalık reddedilmeli", "EUR", 9, true},
		{"Boşluklar temizlenerek geçerli", " TRY ", 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curr, err := domain.NewCurrency(tt.code, tt.decimals)
			if tt.expectErr {
				if err == nil {
					t.Errorf("hata bekleniyordu ama alınmadı: %s", tt.code)
				}
			} else {
				if err != nil {
					t.Fatalf("beklenmeyen hata: %v", err)
				}
				if curr.Decimals() != tt.decimals {
					t.Errorf("ondalık uyuşmuyor: beklenen %d, alınan %d", tt.decimals, curr.Decimals())
				}
			}
		})
	}
}

func TestMoney_TypeSafety_CurrencyMismatch(t *testing.T) {
	mUSD := domain.MustNewMoney(1000, domain.USD)
	mEUR := domain.MustNewMoney(1000, domain.EUR)

	// Farklı para birimlerinin toplanması engellenmeli
	_, err := mUSD.Add(mEUR)
	if err == nil || err != domain.ErrCurrencyMismatch && !isError(err, domain.ErrCurrencyMismatch) {
		t.Errorf("USD ve EUR toplanırken ErrCurrencyMismatch bekleniyordu, alınan: %v", err)
	}

	// Farklı para birimlerinin çıkarılması engellenmeli
	_, err = mUSD.Sub(mEUR)
	if err == nil || !isError(err, domain.ErrCurrencyMismatch) {
		t.Errorf("USD ve EUR çıkarılırken ErrCurrencyMismatch bekleniyordu, alınan: %v", err)
	}

	// Farklı para birimlerinin karşılaştırılması engellenmeli
	_, err = mUSD.Compare(mEUR)
	if err == nil || !isError(err, domain.ErrCurrencyMismatch) {
		t.Errorf("USD ve EUR karşılaştırılırken ErrCurrencyMismatch bekleniyordu, alınan: %v", err)
	}
}

func TestMoney_Arithmetic_AdditionAndSubtraction(t *testing.T) {
	m1 := domain.MustNewMoney(1500, domain.USD) // 15.00 USD
	m2 := domain.MustNewMoney(500, domain.USD)  // 5.00 USD

	// Toplama
	sum, err := m1.Add(m2)
	if err != nil {
		t.Fatalf("toplama hatası: %v", err)
	}
	if sum.Amount() != 2000 {
		t.Errorf("beklenen 2000, alınan %d", sum.Amount())
	}

	// Çıkarma
	diff, err := m1.Sub(m2)
	if err != nil {
		t.Fatalf("çıkarma hatası: %v", err)
	}
	if diff.Amount() != 1000 {
		t.Errorf("beklenen 1000, alınan %d", diff.Amount())
	}
}

func TestMoney_OverflowProtection(t *testing.T) {
	maxMoney := domain.MustNewMoney(math.MaxInt64, domain.USD)
	minMoney := domain.MustNewMoney(math.MinInt64, domain.USD)
	one := domain.MustNewMoney(1, domain.USD)

	// Toplama taşması
	_, err := maxMoney.Add(one)
	if err == nil || !isError(err, domain.ErrAmountOverflow) {
		t.Errorf("MaxInt64 + 1 için ErrAmountOverflow bekleniyordu, alınan: %v", err)
	}

	// Çıkarma taban taşması
	_, err = minMoney.Sub(one)
	if err == nil || !isError(err, domain.ErrAmountOverflow) {
		t.Errorf("MinInt64 - 1 için ErrAmountOverflow bekleniyordu, alınan: %v", err)
	}

	// Negation overflow
	_, err = minMoney.Negate()
	if err == nil || !isError(err, domain.ErrAmountOverflow) {
		t.Errorf("-MinInt64 için ErrAmountOverflow bekleniyordu, alınan: %v", err)
	}

	// Çarpma taşması
	_, err = maxMoney.Mul(2)
	if err == nil || !isError(err, domain.ErrAmountOverflow) {
		t.Errorf("MaxInt64 * 2 için ErrAmountOverflow bekleniyordu, alınan: %v", err)
	}
}

func TestMoney_NewPositiveMoney(t *testing.T) {
	_, err := domain.NewPositiveMoney(100, domain.TRY)
	if err != nil {
		t.Errorf("pozitif tutar kabul edilmeliydi: %v", err)
	}

	_, err = domain.NewPositiveMoney(0, domain.TRY)
	if err == nil || !isError(err, domain.ErrNonPositiveMoney) {
		t.Errorf("0 tutarı için ErrNonPositiveMoney bekleniyordu, alınan: %v", err)
	}

	_, err = domain.NewPositiveMoney(-50, domain.TRY)
	if err == nil || !isError(err, domain.ErrNonPositiveMoney) {
		t.Errorf("negatif tutar için ErrNonPositiveMoney bekleniyordu, alınan: %v", err)
	}
}

func TestMoney_Formatting(t *testing.T) {
	cases := []struct {
		amount   int64
		currency domain.Currency
		expected string
	}{
		{1050, domain.USD, "10.50 USD"},
		{5, domain.USD, "0.05 USD"},
		{0, domain.USD, "0.00 USD"},
		{-1050, domain.USD, "-10.50 USD"},
		{2500, domain.JPY, "2500 JPY"},
	}

	for _, c := range cases {
		m := domain.MustNewMoney(c.amount, c.currency)
		if m.String() != c.expected {
			t.Errorf("beklenen %q, alınan %q", c.expected, m.String())
		}
	}
}

func isError(err, target error) bool {
	if err == nil {
		return target == nil
	}
	// errors.Is kontrolü
	for {
		if err == target {
			return true
		}
		type unwrapper interface {
			Unwrap() error
		}
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
}
