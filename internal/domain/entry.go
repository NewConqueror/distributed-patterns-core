package domain

import (
	"fmt"
	"strings"
)

// Direction, bir muhasebe kaydının yönünü (Borç veya Alacak) ifade eder.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"  // Borç
	DirectionCredit Direction = "CREDIT" // Alacak
)

// IsValid, yönün DEBIT veya CREDIT olup olmadığını doğrular.
func (d Direction) IsValid() bool {
	return d == DirectionDebit || d == DirectionCredit
}

// Opposite, yönün tam tersini döner (Ters işlem / Reversal için).
func (d Direction) Opposite() Direction {
	if d == DirectionDebit {
		return DirectionCredit
	}
	return DirectionDebit
}

// String, yönün dize gösterimini döner.
func (d Direction) String() string {
	return string(d)
}

// Entry, bir işlem içerisindeki tekil bir borç veya alacak kaydını temsil eden değişmez (immutable) nesnedir.
type Entry struct {
	id        string
	accountID string
	direction Direction
	money     Money
}

// NewEntry, doğrulanmış verilerle yeni bir Entry nesnesi oluşturur.
// Tutar kesinlikle sıfırdan büyük (> 0) olmalıdır (finansal kural).
func NewEntry(id, accountID string, direction Direction, money Money) (Entry, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return Entry{}, fmt.Errorf("girdi ID'si boş olamaz")
	}

	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return Entry{}, ErrEmptyAccountID
	}

	if !direction.IsValid() {
		return Entry{}, fmt.Errorf("geçersiz yön: %s (sadece DEBIT veya CREDIT kabul edilir)", direction)
	}

	if money.IsZero() {
		return Entry{}, ErrZeroEntryAmount
	}

	if money.IsNegative() {
		return Entry{}, ErrNegativeMoney
	}

	return Entry{
		id:        id,
		accountID: accountID,
		direction: direction,
		money:     money,
	}, nil
}

// ID, girdinin benzersiz tanımlayıcısını döner.
func (e Entry) ID() string {
	return e.id
}

// AccountID, girdinin ait olduğu hesap ID'sini döner.
func (e Entry) AccountID() string {
	return e.accountID
}

// Direction, girdinin yönünü (DEBIT veya CREDIT) döner.
func (e Entry) Direction() Direction {
	return e.direction
}

// Money, girdinin tutar ve para birimi nesnesini döner.
func (e Entry) Money() Money {
	return e.money
}
