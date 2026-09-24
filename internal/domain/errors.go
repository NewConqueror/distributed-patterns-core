package domain

import "errors"

var (
	// Para ve Değer Nesnesi Hataları
	ErrCurrencyMismatch = errors.New("para birimleri uyuşmuyor")
	ErrInvalidCurrency  = errors.New("geçersiz para birimi kodu")
	ErrAmountOverflow   = errors.New("tutar int64 sınırlarını aştı (overflow/underflow)")
	ErrNonPositiveMoney = errors.New("tutar sıfır veya negatif olamaz")
	ErrNegativeMoney    = errors.New("tutar negatif olamaz")

	// Çift Taraflı Muhasebe Hataları
	ErrUnbalancedTransaction = errors.New("işlem dengesiz: borç (debit) ve alacak (credit) toplamları eşit olmalı")
	ErrInsufficientEntries   = errors.New("işlem en az iki girdi (entry) içermelidir")
	ErrMissingDebitOrCredit  = errors.New("işlem en az bir borç (debit) ve en az bir alacak (credit) içermelidir")
	ErrInvalidAccountType    = errors.New("geçersiz hesap türü")
	ErrEmptyAccountID        = errors.New("hesap ID boş olamaz")
	ErrEmptyTransactionID    = errors.New("işlem ID boş olamaz")
	ErrZeroEntryAmount       = errors.New("girdi tutarı sıfır olamaz")

	// Ters İşlem (Reversal) Hataları
	ErrTransactionAlreadyReversed = errors.New("bu işlem zaten ters kaydedilmiş (reversal yapılmış)")
	ErrCannotReverseReversal      = errors.New("ters işlem (reversal transaction) tekrar tersine çevrilemez")
	ErrOriginalTransactionNil     = errors.New("orijinal işlem boş (nil) olamaz")
)
