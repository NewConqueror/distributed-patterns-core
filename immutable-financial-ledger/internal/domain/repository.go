package domain

import (
	"context"
)

// LedgerRepository, çift taraflı muhasebe sisteminin kalıcılık (persistence) sözleşmesidir.
// Finansal değişmezlik (immutability) ve denetim izi (audit trail) gereği:
// - ASLA UpdateTransaction veya DeleteTransaction metotları İÇERMEZ.
// - Sistem kesinlikle salt-eklemeli (Append-Only) bir modelle çalışır.
// - Düzeltmeler yalnızca CreateReversalTransaction ile sisteme yeni kayıt eklenerek yapılır.
type LedgerRepository interface {
	// SaveTransaction, yeni bir işlemi deftere ekler (Append-only).
	SaveTransaction(ctx context.Context, tx Transaction) error

	// GetTransactionByID, işlem ID'sine göre kaydı getirir.
	GetTransactionByID(ctx context.Context, id string) (Transaction, error)

	// GetTransactionsByAccountID, belirli bir hesaba ait tüm işlem girdilerini getirir.
	GetTransactionsByAccountID(ctx context.Context, accountID string) ([]Transaction, error)

	// SaveAccount, yeni bir hesap açar.
	SaveAccount(ctx context.Context, account Account) error

	// GetAccountByID, hesap ID'sine göre hesabı getirir.
	GetAccountByID(ctx context.Context, id string) (Account, error)

	// ListAccounts, sistemdeki tüm hesapları listeler (muhasebe denkliği kontrolü vb. için).
	ListAccounts(ctx context.Context) ([]Account, error)
}
