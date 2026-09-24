package domain

import (
	"fmt"
	"strings"
)

// AccountType, muhasebedeki 5 temel hesap türünü temsil eder.
type AccountType string

const (
	AccountTypeAsset     AccountType = "ASSET"     // Varlık: Kasa, Banka, Alacaklar vb. (Normal Bakiye: DEBIT)
	AccountTypeLiability AccountType = "LIABILITY" // Yükümlülük: Borçlar, Krediler vb. (Normal Bakiye: CREDIT)
	AccountTypeEquity    AccountType = "EQUITY"    // Özkaynak: Sermaye vb. (Normal Bakiye: CREDIT)
	AccountTypeRevenue   AccountType = "REVENUE"   // Gelir: Satışlar, Faiz Gelirleri vb. (Normal Bakiye: CREDIT)
	AccountTypeExpense   AccountType = "EXPENSE"   // Gider: Kira, Maaş, Maliyetler vb. (Normal Bakiye: DEBIT)
)

// IsValid, hesap türünün geçerli 5 türden biri olup olmadığını kontrol eder.
func (at AccountType) IsValid() bool {
	switch at {
	case AccountTypeAsset, AccountTypeLiability, AccountTypeEquity, AccountTypeRevenue, AccountTypeExpense:
		return true
	default:
		return false
	}
}

// NormalBalance, hesap türünün normal bakiye yönünü (Debit veya Credit) döner.
// Assets ve Expenses -> DEBIT normal
// Liabilities, Equity ve Revenue -> CREDIT normal
func (at AccountType) NormalBalance() Direction {
	switch at {
	case AccountTypeAsset, AccountTypeExpense:
		return DirectionDebit
	case AccountTypeLiability, AccountTypeEquity, AccountTypeRevenue:
		return DirectionCredit
	default:
		return ""
	}
}

// Account, bir defteri kebir (ledger) hesabını temsil eden değişmez (immutable) domain modelidir.
type Account struct {
	id          string
	name        string
	accountType AccountType
	currency    Currency
	balance     Money // Normal bakiye yönüne göre net bakiye
}

// NewAccount, geçerli verilerle yeni bir Account nesnesi oluşturur (başlangıç bakiyesi 0).
func NewAccount(id, name string, accountType AccountType, currency Currency) (Account, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return Account{}, ErrEmptyAccountID
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return Account{}, fmt.Errorf("hesap adı boş olamaz")
	}
	if !accountType.IsValid() {
		return Account{}, fmt.Errorf("%w: %s", ErrInvalidAccountType, accountType)
	}
	if currency.IsZero() {
		return Account{}, ErrInvalidCurrency
	}

	zeroMoney, err := NewZeroMoney(currency)
	if err != nil {
		return Account{}, err
	}

	return Account{
		id:          id,
		name:        name,
		accountType: accountType,
		currency:    currency,
		balance:     zeroMoney,
	}, nil
}

// ID, hesabın benzersiz tanımlayıcısını döner.
func (a Account) ID() string {
	return a.id
}

// Name, hesabın adını döner.
func (a Account) Name() string {
	return a.name
}

// AccountType, hesabın muhasebe türünü döner.
func (a Account) AccountType() AccountType {
	return a.accountType
}

// Currency, hesabın para birimini döner.
func (a Account) Currency() Currency {
	return a.currency
}

// Balance, hesabın güncel bakiyesini döner.
func (a Account) Balance() Money {
	return a.balance
}

// ApplyEntry, bir işlem girdisini hesaba uygulayarak yeni bir Account kopyası döner (Pure Domain/Immutability).
// Normal bakiyeye göre:
// - Eğer girdinin yönü hesabın normal yönü ile aynıysa bakiye artar.
// - Eğer girdinin yönü zıtsa bakiye azalır.
func (a Account) ApplyEntry(entry Entry) (Account, error) {
	if entry.AccountID() != a.id {
		return Account{}, fmt.Errorf("girdi hesap ID'si (%s) ile hedef hesap ID'si (%s) uyuşmuyor", entry.AccountID(), a.id)
	}
	if !entry.Money().Currency().Equals(a.currency) {
		return Account{}, fmt.Errorf("%w: girdi %s, hesap %s", ErrCurrencyMismatch, entry.Money().Currency().Code(), a.currency.Code())
	}

	var newBalance Money
	var err error

	if entry.Direction() == a.accountType.NormalBalance() {
		// Normal bakiye yönünde hareket -> Bakiyeyi arttır
		newBalance, err = a.balance.Add(entry.Money())
	} else {
		// Ters yönde hareket -> Bakiyeyi azalt
		newBalance, err = a.balance.Sub(entry.Money())
	}

	if err != nil {
		return Account{}, fmt.Errorf("bakiye güncellenirken hata oluştu: %w", err)
	}

	updated := a
	updated.balance = newBalance
	return updated, nil
}
