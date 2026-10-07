package domain_test

import (
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

func TestTransaction_Creation_Success(t *testing.T) {
	amount := domain.MustNewMoney(5000, domain.USD) // 50.00 USD

	e1, err := domain.NewEntry("e1", "acc-asset", domain.DirectionDebit, amount)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := domain.NewEntry("e2", "acc-liability", domain.DirectionCredit, amount)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := domain.NewTransaction("tx-1", time.Now().UTC(), "Banka Kredisi Çekimi", []domain.Entry{e1, e2})
	if err != nil {
		t.Fatalf("işlem oluşturulamadı: %v", err)
	}

	if tx.ID() != "tx-1" {
		t.Errorf("işlem ID uyuşmuyor: %s", tx.ID())
	}
	if tx.TotalAmount().Amount() != 5000 {
		t.Errorf("toplam tutar uyuşmuyor: %d", tx.TotalAmount().Amount())
	}
	if tx.IsReversal() {
		t.Errorf("yeni işlem reversal olmamalı")
	}
	if tx.ReversalOfID() != nil {
		t.Errorf("reversalOfID nil olmalı")
	}
}

func TestTransaction_UnbalancedTransaction_Rejected(t *testing.T) {
	amount1 := domain.MustNewMoney(5000, domain.USD) // 50.00 USD Debit
	amount2 := domain.MustNewMoney(4500, domain.USD) // 45.00 USD Credit (Fark: 5.00 USD)

	e1, _ := domain.NewEntry("e1", "acc-1", domain.DirectionDebit, amount1)
	e2, _ := domain.NewEntry("e2", "acc-2", domain.DirectionCredit, amount2)

	_, err := domain.NewTransaction("tx-unbalanced", time.Now().UTC(), "Dengesiz İşlem", []domain.Entry{e1, e2})
	if err == nil || !isError(err, domain.ErrUnbalancedTransaction) {
		t.Fatalf("dengesiz işlem reddedilmeliydi (ErrUnbalancedTransaction), alınan: %v", err)
	}
}

func TestTransaction_InsufficientEntries_Rejected(t *testing.T) {
	amount := domain.MustNewMoney(1000, domain.USD)
	e1, _ := domain.NewEntry("e1", "acc-1", domain.DirectionDebit, amount)

	// Tek girdili işlem
	_, err := domain.NewTransaction("tx-single", time.Now().UTC(), "Tek girdi", []domain.Entry{e1})
	if err == nil || !isError(err, domain.ErrInsufficientEntries) {
		t.Fatalf("tek girdi için ErrInsufficientEntries bekleniyordu, alınan: %v", err)
	}

	// 0 girdili işlem
	_, err = domain.NewTransaction("tx-empty", time.Now().UTC(), "Sıfır girdi", []domain.Entry{})
	if err == nil || !isError(err, domain.ErrInsufficientEntries) {
		t.Fatalf("boş girdiler için ErrInsufficientEntries bekleniyordu, alınan: %v", err)
	}
}

func TestTransaction_MissingDebitOrCredit_Rejected(t *testing.T) {
	amount := domain.MustNewMoney(1000, domain.USD)

	// Sadece iki Debit
	e1, _ := domain.NewEntry("e1", "acc-1", domain.DirectionDebit, amount)
	e2, _ := domain.NewEntry("e2", "acc-2", domain.DirectionDebit, amount)

	_, err := domain.NewTransaction("tx-all-debit", time.Now().UTC(), "Tümü Borç", []domain.Entry{e1, e2})
	if err == nil || !isError(err, domain.ErrMissingDebitOrCredit) {
		t.Fatalf("sadece borç içeren işlem reddedilmeliydi, alınan: %v", err)
	}

	// Sadece iki Credit
	e3, _ := domain.NewEntry("e3", "acc-1", domain.DirectionCredit, amount)
	e4, _ := domain.NewEntry("e4", "acc-2", domain.DirectionCredit, amount)

	_, err = domain.NewTransaction("tx-all-credit", time.Now().UTC(), "Tümü Alacak", []domain.Entry{e3, e4})
	if err == nil || !isError(err, domain.ErrMissingDebitOrCredit) {
		t.Fatalf("sadece alacak içeren işlem reddedilmeliydi, alınan: %v", err)
	}
}

func TestTransaction_CurrencyMismatchInEntries_Rejected(t *testing.T) {
	amountUSD := domain.MustNewMoney(1000, domain.USD)
	amountEUR := domain.MustNewMoney(1000, domain.EUR)

	e1, _ := domain.NewEntry("e1", "acc-1", domain.DirectionDebit, amountUSD)
	e2, _ := domain.NewEntry("e2", "acc-2", domain.DirectionCredit, amountEUR)

	_, err := domain.NewTransaction("tx-diff-curr", time.Now().UTC(), "Farklı Para Birimi", []domain.Entry{e1, e2})
	if err == nil || !isError(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("farklı para birimli girdiler reddedilmeliydi, alınan: %v", err)
	}
}

func TestTransaction_ZeroOrNegativeEntryAmount_Rejected(t *testing.T) {
	zeroMoney := domain.MustNewMoney(0, domain.USD)
	_, err := domain.NewEntry("e-zero", "acc-1", domain.DirectionDebit, zeroMoney)
	if err == nil || !isError(err, domain.ErrZeroEntryAmount) {
		t.Fatalf("sıfır tutarlı girdi reddedilmeliydi, alınan: %v", err)
	}

	negMoney := domain.MustNewMoney(-100, domain.USD)
	_, err = domain.NewEntry("e-neg", "acc-1", domain.DirectionDebit, negMoney)
	if err == nil || !isError(err, domain.ErrNegativeMoney) {
		t.Fatalf("negatif tutarlı girdi reddedilmeliydi, alınan: %v", err)
	}
}

func TestTransaction_DefensiveCopy(t *testing.T) {
	amount := domain.MustNewMoney(1000, domain.USD)
	e1, _ := domain.NewEntry("e1", "acc-1", domain.DirectionDebit, amount)
	e2, _ := domain.NewEntry("e2", "acc-2", domain.DirectionCredit, amount)

	entries := []domain.Entry{e1, e2}
	tx, err := domain.NewTransaction("tx-def", time.Now().UTC(), "Kopya Testi", entries)
	if err != nil {
		t.Fatal(err)
	}

	// Dışarıdaki dilimi modifiye et
	entries[0] = e2
	if tx.Entries()[0].Direction() != domain.DirectionDebit {
		t.Errorf("orijinal girdi dilimi mutasyona uğradı, immutability ihlali!")
	}

	// Alınan kopyayı modifiye et
	txEntries := tx.Entries()
	txEntries[0] = e2
	if tx.Entries()[0].Direction() != domain.DirectionDebit {
		t.Errorf("tx.Entries() mutasyona izin verdi, immutability ihlali!")
	}
}

func TestTransaction_CreateReversalTransaction(t *testing.T) {
	amount := domain.MustNewMoney(7500, domain.USD)
	e1, _ := domain.NewEntry("e1", "acc-asset", domain.DirectionDebit, amount)
	e2, _ := domain.NewEntry("e2", "acc-revenue", domain.DirectionCredit, amount)

	origTx, err := domain.NewTransaction("tx-orig-100", time.Now().UTC(), "Orijinal Satış Kaydı", []domain.Entry{e1, e2})
	if err != nil {
		t.Fatal(err)
	}

	// Ters işlem oluştur
	revTx, err := domain.CreateReversalTransaction(origTx, "tx-rev-100", time.Now().UTC(), "Hatalı fatura iptali")
	if err != nil {
		t.Fatalf("reversal oluşturulamadı: %v", err)
	}

	// Doğrulamalar:
	if !revTx.IsReversal() {
		t.Errorf("revTx.IsReversal() true olmalıdır")
	}
	if revTx.ReversalOfID() == nil || *revTx.ReversalOfID() != "tx-orig-100" {
		t.Errorf("reversal_of_id tx-orig-100 olmalıdır, alınan: %v", revTx.ReversalOfID())
	}
	if revTx.TotalAmount().Amount() != origTx.TotalAmount().Amount() {
		t.Errorf("reversal toplamı orijinalle aynı olmalı")
	}

	revEntries := revTx.Entries()
	if len(revEntries) != 2 {
		t.Fatalf("2 girdi bekleniyordu, alınan %d", len(revEntries))
	}

	// Yönler tam tersine dönmüş olmalı:
	// Orijinal e1: acc-asset -> DEBIT => Tersinde: acc-asset -> CREDIT
	if revEntries[0].AccountID() != "acc-asset" || revEntries[0].Direction() != domain.DirectionCredit {
		t.Errorf("1. girdinin yönü CREDIT olmalıydı, alınan: %s", revEntries[0].Direction())
	}
	// Orijinal e2: acc-revenue -> CREDIT => Tersinde: acc-revenue -> DEBIT
	if revEntries[1].AccountID() != "acc-revenue" || revEntries[1].Direction() != domain.DirectionDebit {
		t.Errorf("2. girdinin yönü DEBIT olmalıydı, alınan: %s", revEntries[1].Direction())
	}

	// Net Bakiye Etkisi Testi: Orijinal + Ters İşlem hesabı net sıfır etkilemeli
	assetAcc, _ := domain.NewAccount("acc-asset", "Varlık Hesabı", domain.AccountTypeAsset, domain.USD)
	assetAcc, _ = assetAcc.ApplyEntry(origTx.Entries()[0])
	if assetAcc.Balance().Amount() != 7500 {
		t.Errorf("orijinal işlem sonrası bakiye 7500 olmalıydı")
	}
	assetAcc, _ = assetAcc.ApplyEntry(revTx.Entries()[0])
	if assetAcc.Balance().Amount() != 0 {
		t.Errorf("ters işlem sonrası bakiye 0 olmalıydı, alınan: %d", assetAcc.Balance().Amount())
	}

	// Bir ters işlemin tekrar tersi alınamaz
	_, err = domain.CreateReversalTransaction(revTx, "tx-rev-rev", time.Now().UTC(), "İkinci ters alma denemesi")
	if err == nil || !isError(err, domain.ErrCannotReverseReversal) {
		t.Fatalf("ters işlemin tersi alınırken ErrCannotReverseReversal bekleniyordu, alınan: %v", err)
	}
}
