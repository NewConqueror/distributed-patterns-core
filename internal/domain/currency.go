package domain

import (
	"fmt"
	"strings"
	"unicode"
)

// Currency, ISO-4217 standartlarına uygun bir para birimini temsil eden değer nesnesidir (Value Object).
type Currency struct {
	code     string
	decimals int
}

var (
	USD = MustNewCurrency("USD", 2)
	EUR = MustNewCurrency("EUR", 2)
	TRY = MustNewCurrency("TRY", 2)
	GBP = MustNewCurrency("GBP", 2)
	JPY = MustNewCurrency("JPY", 0)
)

// NewCurrency, geçerli bir para birimi kodu ve ondalık basamak (decimals) ile Currency nesnesi oluşturur.
func NewCurrency(code string, decimals int) (Currency, error) {
	code = strings.TrimSpace(code)
	if len(code) != 3 {
		return Currency{}, fmt.Errorf("%w: para birimi 3 karakterden oluşmalıdır (aldı: %s)", ErrInvalidCurrency, code)
	}

	for _, r := range code {
		if !unicode.IsUpper(r) || !unicode.IsLetter(r) {
			return Currency{}, fmt.Errorf("%w: para birimi kodu büyük harflerden oluşmalıdır (aldı: %s)", ErrInvalidCurrency, code)
		}
	}

	if decimals < 0 || decimals > 8 {
		return Currency{}, fmt.Errorf("%w: ondalık basamak sayısı 0 ile 8 arasında olmalıdır (aldı: %d)", ErrInvalidCurrency, decimals)
	}

	return Currency{code: code, decimals: decimals}, nil
}

// MustNewCurrency, hata durumunda panic fırlatan Currency oluşturucu (sabitler için).
func MustNewCurrency(code string, decimals int) Currency {
	c, err := NewCurrency(code, decimals)
	if err != nil {
		panic(err)
	}
	return c
}

// Code, para biriminin 3 harfli kodunu döner.
func (c Currency) Code() string {
	return c.code
}

// Decimals, para biriminin en küçük birim ondalık basamak sayısını döner.
func (c Currency) Decimals() int {
	return c.decimals
}

// Equals, iki para biriminin eşitliğini kontrol eder.
func (c Currency) Equals(other Currency) bool {
	return c.code == other.code && c.decimals == other.decimals
}

// IsZero, para biriminin ilklendirilip ilklendirilmediğini bildirir.
func (c Currency) IsZero() bool {
	return c.code == ""
}

// String, para biriminin dize gösterimini döner.
func (c Currency) String() string {
	return c.code
}
