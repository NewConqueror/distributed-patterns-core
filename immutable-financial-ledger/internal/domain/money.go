package domain

import (
	"fmt"
	"math"
	"strings"
)

// Money, çift taraflı muhasebe sisteminde parasal değeri temsil eden değişmez (immutable) Değer Nesnesidir (Value Object).
// Kayan noktalı sayı (float/double) kesinlikle kullanılmaz; değer en küçük para biriminde (ör. cent, kuruş) int64 olarak tutulur.
type Money struct {
	amount   int64
	currency Currency
}

// NewMoney, belirtilen tutar ve para birimi ile yeni bir Money nesnesi oluşturur.
func NewMoney(amount int64, currency Currency) (Money, error) {
	if currency.IsZero() {
		return Money{}, ErrInvalidCurrency
	}
	return Money{
		amount:   amount,
		currency: currency,
	}, nil
}

// MustNewMoney, hata durumunda panic fırlatan Money oluşturucu (testler veya doğrulanmış girdiler için).
func MustNewMoney(amount int64, currency Currency) Money {
	m, err := NewMoney(amount, currency)
	if err != nil {
		panic(err)
	}
	return m
}

// NewPositiveMoney, tutarın kesinlikle sıfırdan büyük (> 0) olmasını zorunlu kılan oluşturucu.
// İşlem girdilerinde (Entries) negatif veya sıfır tutar engellenmesinde kullanılır.
func NewPositiveMoney(amount int64, currency Currency) (Money, error) {
	if amount <= 0 {
		return Money{}, fmt.Errorf("%w: tutar %d verildi, > 0 olmalı", ErrNonPositiveMoney, amount)
	}
	return NewMoney(amount, currency)
}

// NewZeroMoney, belirtilen para biriminde 0 tutarlı bir Money nesnesi döner.
func NewZeroMoney(currency Currency) (Money, error) {
	return NewMoney(0, currency)
}

// Amount, paranın en küçük birimdeki tam sayı (int64) tutarını döner.
func (m Money) Amount() int64 {
	return m.amount
}

// Currency, paranın para birimini döner.
func (m Money) Currency() Currency {
	return m.currency
}

// IsZero, tutarın 0 olup olmadığını döner.
func (m Money) IsZero() bool {
	return m.amount == 0
}

// IsPositive, tutarın 0'dan büyük olup olmadığını döner.
func (m Money) IsPositive() bool {
	return m.amount > 0
}

// IsNegative, tutarın 0'dan küçük olup olmadığını döner.
func (m Money) IsNegative() bool {
	return m.amount < 0
}

// Equals, iki Money nesnesinin hem para birimi hem de tutar olarak eşit olup olmadığını kontrol eder.
func (m Money) Equals(other Money) bool {
	return m.currency.Equals(other.currency) && m.amount == other.amount
}

// ensureSameCurrency, iki para biriminin aynı olup olmadığını doğrular.
func (m Money) ensureSameCurrency(other Money) error {
	if !m.currency.Equals(other.currency) {
		return fmt.Errorf("%w: %s ile %s birleştirilemez", ErrCurrencyMismatch, m.currency.Code(), other.currency.Code())
	}
	return nil
}

// Add, iki Money nesnesini toplar. Para birimleri farklıysa ErrCurrencyMismatch, int64 taşması olursa ErrAmountOverflow döner.
func (m Money) Add(other Money) (Money, error) {
	if err := m.ensureSameCurrency(other); err != nil {
		return Money{}, err
	}

	// Taşma (Overflow) ve Taban Taşması (Underflow) denetimi
	if other.amount > 0 && m.amount > math.MaxInt64-other.amount {
		return Money{}, fmt.Errorf("%w: toplama taşması (%d + %d)", ErrAmountOverflow, m.amount, other.amount)
	}
	if other.amount < 0 && m.amount < math.MinInt64-other.amount {
		return Money{}, fmt.Errorf("%w: çıkarma taban taşması (%d + %d)", ErrAmountOverflow, m.amount, other.amount)
	}

	return Money{
		amount:   m.amount + other.amount,
		currency: m.currency,
	}, nil
}

// Sub, mevcut tutardan diğer tutarı çıkarır.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.ensureSameCurrency(other); err != nil {
		return Money{}, err
	}

	// a - b taşma denetimi
	if other.amount < 0 && m.amount > math.MaxInt64+other.amount {
		return Money{}, fmt.Errorf("%w: çıkarma taşması (%d - %d)", ErrAmountOverflow, m.amount, other.amount)
	}
	if other.amount > 0 && m.amount < math.MinInt64+other.amount {
		return Money{}, fmt.Errorf("%w: çıkarma taban taşması (%d - %d)", ErrAmountOverflow, m.amount, other.amount)
	}

	return Money{
		amount:   m.amount - other.amount,
		currency: m.currency,
	}, nil
}

// Mul, tutarı bir tam sayı çarpanı ile çarpar. Taşma durumunda ErrAmountOverflow döner.
func (m Money) Mul(factor int64) (Money, error) {
	if m.amount == 0 || factor == 0 {
		return Money{amount: 0, currency: m.currency}, nil
	}

	result := m.amount * factor
	if m.amount != result/factor {
		return Money{}, fmt.Errorf("%w: çarpma taşması (%d * %d)", ErrAmountOverflow, m.amount, factor)
	}

	return Money{
		amount:   result,
		currency: m.currency,
	}, nil
}

// Negate, tutarın işaretini tersine çevirir (+ ise -, - ise +).
func (m Money) Negate() (Money, error) {
	if m.amount == math.MinInt64 {
		return Money{}, fmt.Errorf("%w: math.MinInt64 tersine çevrilemez", ErrAmountOverflow)
	}
	return Money{
		amount:   -m.amount,
		currency: m.currency,
	}, nil
}

// Abs, tutarın mutlak değerini döner.
func (m Money) Abs() (Money, error) {
	if m.amount >= 0 {
		return m, nil
	}
	return m.Negate()
}

// Compare, iki Money nesnesini karşılaştırır.
// m < other -> -1
// m == other -> 0
// m > other -> 1
func (m Money) Compare(other Money) (int, error) {
	if err := m.ensureSameCurrency(other); err != nil {
		return 0, err
	}
	if m.amount < other.amount {
		return -1, nil
	}
	if m.amount > other.amount {
		return 1, nil
	}
	return 0, nil
}

// GreaterThan, m'nin other'dan büyük olup olmadığını döner.
func (m Money) GreaterThan(other Money) (bool, error) {
	cmp, err := m.Compare(other)
	if err != nil {
		return false, err
	}
	return cmp > 0, nil
}

// LessThan, m'nin other'dan küçük olup olmadığını döner.
func (m Money) LessThan(other Money) (bool, error) {
	cmp, err := m.Compare(other)
	if err != nil {
		return false, err
	}
	return cmp < 0, nil
}

// GreaterThanOrEqual, m'nin other'dan büyük veya eşit olup olmadığını döner.
func (m Money) GreaterThanOrEqual(other Money) (bool, error) {
	cmp, err := m.Compare(other)
	if err != nil {
		return false, err
	}
	return cmp >= 0, nil
}

// LessThanOrEqual, m'nin other'dan küçük veya eşit olup olmadığını döner.
func (m Money) LessThanOrEqual(other Money) (bool, error) {
	cmp, err := m.Compare(other)
	if err != nil {
		return false, err
	}
	return cmp <= 0, nil
}

// String, Money nesnesinin standart dize temsilini döner (örneğin "10.50 USD", "-5.00 EUR").
func (m Money) String() string {
	decimals := m.currency.Decimals()
	if decimals == 0 {
		return fmt.Sprintf("%d %s", m.amount, m.currency.Code())
	}

	isNegative := m.amount < 0
	absAmount := m.amount
	if isNegative {
		// math.MinInt64 için absAmount overflow koruması
		if absAmount == math.MinInt64 {
			absAmount = math.MaxInt64
		} else {
			absAmount = -absAmount
		}
	}

	divisor := int64(1)
	for i := 0; i < decimals; i++ {
		divisor *= 10
	}

	integerPart := absAmount / divisor
	fractionalPart := absAmount % divisor

	sign := ""
	if isNegative {
		sign = "-"
	}

	formatStr := fmt.Sprintf("%%s%%d.%%0%dd %%s", decimals)
	return strings.TrimSpace(fmt.Sprintf(formatStr, sign, integerPart, fractionalPart, m.currency.Code()))
}
