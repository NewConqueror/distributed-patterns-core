package domain

import (
	"fmt"
	"strings"
	"time"
)

// Transaction, çift taraflı muhasebe sistemindeki bir finansal işlemi temsil eden değişmez (immutable) nesnedir.
// Bir işlem oluşturulduktan sonra asla güncellenemez veya silinemez (Append-only).
type Transaction struct {
	id           string
	timestamp    time.Time
	description  string
	entries      []Entry
	currency     Currency
	totalAmount  Money
	reversalOfID *string
}

// NewTransaction, çift taraflı muhasebe kurallarını ve değişmezlikleri (invariants) doğrulayarak yeni bir işlem yaratır.
// Kurallar:
// 1. İşlem ID boş olamaz.
// 2. En az 2 girdi (entry) bulunmalıdır (biri Borç/Debit, diğeri Alacak/Credit).
// 3. Tüm girdiler aynı para biriminde olmalıdır.
// 4. Her girdinin tutarı kesinlikle pozitif (> 0) olmalıdır.
// 5. Borç ve Alacak toplamı eşit olmalıdır: sum(Debit) - sum(Credit) = 0.
func NewTransaction(id string, timestamp time.Time, description string, entries []Entry) (Transaction, error) {
	return newTransactionInternal(id, timestamp, description, entries, nil)
}

// newTransactionInternal, dahili işlem oluşturucu (normal işlem ve ters işlem için ortak doğrulama).
func newTransactionInternal(id string, timestamp time.Time, description string, entries []Entry, reversalOfID *string) (Transaction, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return Transaction{}, ErrEmptyTransactionID
	}

	if len(entries) < 2 {
		return Transaction{}, fmt.Errorf("%w: işlem en az 2 girdi gerektirir, verilen: %d", ErrInsufficientEntries, len(entries))
	}

	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	// İlk girdinin para birimini referans alıyoruz
	firstCurrency := entries[0].Money().Currency()
	totalDebit, err := NewZeroMoney(firstCurrency)
	if err != nil {
		return Transaction{}, err
	}
	totalCredit, err := NewZeroMoney(firstCurrency)
	if err != nil {
		return Transaction{}, err
	}

	hasDebit := false
	hasCredit := false

	// Girdilerin savunmacı kopyası (defensive copy)
	copiedEntries := make([]Entry, len(entries))

	for i, entry := range entries {
		// Para birimi tutarlılığı denetimi
		if !entry.Money().Currency().Equals(firstCurrency) {
			return Transaction{}, fmt.Errorf("%w: girdi %s para birimi (%s), beklenen (%s)",
				ErrCurrencyMismatch, entry.ID(), entry.Money().Currency().Code(), firstCurrency.Code())
		}

		// Yön toplamlarını biriktir
		switch entry.Direction() {
		case DirectionDebit:
			hasDebit = true
			totalDebit, err = totalDebit.Add(entry.Money())
			if err != nil {
				return Transaction{}, fmt.Errorf("borç (debit) toplamı hesaplanırken taşma: %w", err)
			}
		case DirectionCredit:
			hasCredit = true
			totalCredit, err = totalCredit.Add(entry.Money())
			if err != nil {
				return Transaction{}, fmt.Errorf("alacak (credit) toplamı hesaplanırken taşma: %w", err)
			}
		default:
			return Transaction{}, fmt.Errorf("geçersiz girdi yönü: %s", entry.Direction())
		}

		copiedEntries[i] = entry
	}

	// En az 1 Borç ve 1 Alacak bulunmalı
	if !hasDebit || !hasCredit {
		return Transaction{}, ErrMissingDebitOrCredit
	}

	// Muhasebe Denkliği Değişmezi (Double-Entry Invariant):
	// sum(Debit) - sum(Credit) = 0
	if !totalDebit.Equals(totalCredit) {
		return Transaction{}, fmt.Errorf("%w: Borç Toplamı: %s, Alacak Toplamı: %s (fark: %d)",
			ErrUnbalancedTransaction, totalDebit.String(), totalCredit.String(), totalDebit.Amount()-totalCredit.Amount())
	}

	var revIDCopy *string
	if reversalOfID != nil {
		v := *reversalOfID
		revIDCopy = &v
	}

	return Transaction{
		id:           id,
		timestamp:    timestamp,
		description:  description,
		entries:      copiedEntries,
		currency:     firstCurrency,
		totalAmount:  totalDebit,
		reversalOfID: revIDCopy,
	}, nil
}

// CreateReversalTransaction, hatalı bir işlemi düzeltmek için orijinal kaydın tam tersi girdilerini üreten yeni bir işlem yaratır.
// Orijinal kaydın Debit girdileri Credit, Credit girdileri ise Debit haline getirilir.
// reversal_of_id ile orijinal işleme bağlanır.
// Immutability gereği orijinal kayıt asla silinmez veya güncellenmez.
func CreateReversalTransaction(orig Transaction, reversalID string, timestamp time.Time, reason string) (Transaction, error) {
	if orig.id == "" {
		return Transaction{}, ErrOriginalTransactionNil
	}

	if orig.IsReversal() {
		return Transaction{}, fmt.Errorf("%w: işlem %s zaten bir ters işlemdir", ErrCannotReverseReversal, orig.id)
	}

	reversalID = strings.TrimSpace(reversalID)
	if reversalID == "" {
		return Transaction{}, ErrEmptyTransactionID
	}

	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	// Orijinal girdilerin tam tersini oluştur
	origEntries := orig.Entries()
	revEntries := make([]Entry, len(origEntries))

	for i, e := range origEntries {
		revEntryID := fmt.Sprintf("%s-rev-%d", reversalID, i+1)
		revDirection := e.Direction().Opposite()

		revEntry, err := NewEntry(revEntryID, e.AccountID(), revDirection, e.Money())
		if err != nil {
			return Transaction{}, fmt.Errorf("ters girdi oluşturulamadı: %w", err)
		}
		revEntries[i] = revEntry
	}

	desc := fmt.Sprintf("Reversal of %s: %s", orig.id, reason)
	origID := orig.id

	return newTransactionInternal(reversalID, timestamp, desc, revEntries, &origID)
}

// ID, işlemin benzersiz kimliğini döner.
func (t Transaction) ID() string {
	return t.id
}

// Timestamp, işlemin gerçekleşme zamanını döner.
func (t Transaction) Timestamp() time.Time {
	return t.timestamp
}

// Description, işlemin açıklamasını döner.
func (t Transaction) Description() string {
	return t.description
}

// Entries, işlem girdilerinin kopyasını döner (değişmezliği korumak için).
func (t Transaction) Entries() []Entry {
	result := make([]Entry, len(t.entries))
	copy(result, t.entries)
	return result
}

// Currency, işlemin para birimini döner.
func (t Transaction) Currency() Currency {
	return t.currency
}

// TotalAmount, işlemin toplam dengeli hacmini döner (sum(Debit) = sum(Credit)).
func (t Transaction) TotalAmount() Money {
	return t.totalAmount
}

// IsReversal, bu işlemin başka bir işlemin tersi (reversal) olup olmadığını belirtir.
func (t Transaction) IsReversal() bool {
	return t.reversalOfID != nil
}

// ReversalOfID, eğer bu bir ters işlem ise orijinal işlem kimliğini döner; değilse nil döner.
func (t Transaction) ReversalOfID() *string {
	if t.reversalOfID == nil {
		return nil
	}
	cp := *t.reversalOfID
	return &cp
}
