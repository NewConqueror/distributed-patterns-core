package domain_test

import (
	"testing"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

func TestAccount_CreationAndValidation(t *testing.T) {
	// Geçerli hesap
	acc, err := domain.NewAccount("acc-1", "Kasa Hesabı", domain.AccountTypeAsset, domain.USD)
	if err != nil {
		t.Fatalf("beklenmeyen hata: %v", err)
	}
	if acc.ID() != "acc-1" || acc.AccountType() != domain.AccountTypeAsset {
		t.Errorf("hesap alanları uyuşmuyor")
	}
	if !acc.Balance().IsZero() {
		t.Errorf("başlangıç bakiyesi sıfır olmalıdır")
	}

	// Boş ID
	_, err = domain.NewAccount("", "Kasa", domain.AccountTypeAsset, domain.USD)
	if err == nil || !isError(err, domain.ErrEmptyAccountID) {
		t.Errorf("boş ID için ErrEmptyAccountID bekleniyordu, alınan: %v", err)
	}

	// Geçersiz hesap türü
	_, err = domain.NewAccount("acc-2", "Bilinmeyen", domain.AccountType("INVALID"), domain.USD)
	if err == nil || !isError(err, domain.ErrInvalidAccountType) {
		t.Errorf("geçersiz hesap türü için ErrInvalidAccountType bekleniyordu, alınan: %v", err)
	}
}

func TestAccount_NormalBalanceAndApplyEntry(t *testing.T) {
	// ASSET hesabı (Normal Balance: DEBIT)
	assetAcc, err := domain.NewAccount("cash", "Nakit Kasa", domain.AccountTypeAsset, domain.USD)
	if err != nil {
		t.Fatal(err)
	}

	// 100 USD Borç (Debit) -> Varlık artar (+100)
	debitMoney := domain.MustNewMoney(10000, domain.USD)
	debitEntry, err := domain.NewEntry("e1", "cash", domain.DirectionDebit, debitMoney)
	if err != nil {
		t.Fatal(err)
	}
	assetAcc, err = assetAcc.ApplyEntry(debitEntry)
	if err != nil {
		t.Fatalf("girdi uygulanamadı: %v", err)
	}
	if assetAcc.Balance().Amount() != 10000 {
		t.Errorf("beklenen bakiye 10000, alınan %d", assetAcc.Balance().Amount())
	}

	// 30 USD Alacak (Credit) -> Varlık azalır (-30, kalan 70)
	creditMoney := domain.MustNewMoney(3000, domain.USD)
	creditEntry, err := domain.NewEntry("e2", "cash", domain.DirectionCredit, creditMoney)
	if err != nil {
		t.Fatal(err)
	}
	assetAcc, err = assetAcc.ApplyEntry(creditEntry)
	if err != nil {
		t.Fatalf("girdi uygulanamadı: %v", err)
	}
	if assetAcc.Balance().Amount() != 7000 {
		t.Errorf("beklenen bakiye 7000, alınan %d", assetAcc.Balance().Amount())
	}

	// LIABILITY hesabı (Normal Balance: CREDIT)
	liabAcc, err := domain.NewAccount("loan", "Banka Kredisi", domain.AccountTypeLiability, domain.USD)
	if err != nil {
		t.Fatal(err)
	}

	// 50 USD Alacak (Credit) -> Borç yükümlülüğü artar (+50)
	creditLiabMoney := domain.MustNewMoney(5000, domain.USD)
	liabEntry, err := domain.NewEntry("e3", "loan", domain.DirectionCredit, creditLiabMoney)
	if err != nil {
		t.Fatal(err)
	}
	liabAcc, err = liabAcc.ApplyEntry(liabEntry)
	if err != nil {
		t.Fatal(err)
	}
	if liabAcc.Balance().Amount() != 5000 {
		t.Errorf("beklenen yükümlülük 5000, alınan %d", liabAcc.Balance().Amount())
	}
}

func TestAccountingEquation_Validation(t *testing.T) {
	// Senaryo:
	// Kurucu 10,000 USD sermaye koydu:
	// Kasa (Asset) +10,000 (Debit)
	// Sermaye (Equity) +10,000 (Credit)
	// Denklem: Assets (10000) = Liabilities (0) + Equity (10000)

	cash, _ := domain.NewAccount("cash", "Kasa", domain.AccountTypeAsset, domain.USD)
	equity, _ := domain.NewAccount("equity", "Sermaye", domain.AccountTypeEquity, domain.USD)

	e1, _ := domain.NewEntry("e1", "cash", domain.DirectionDebit, domain.MustNewMoney(1000000, domain.USD))
	e2, _ := domain.NewEntry("e2", "equity", domain.DirectionCredit, domain.MustNewMoney(1000000, domain.USD))

	cash, _ = cash.ApplyEntry(e1)
	equity, _ = equity.ApplyEntry(e2)

	accounts := []domain.Account{cash, equity}
	report, err := domain.ValidateAccountingEquation(accounts, domain.USD)
	if err != nil {
		t.Fatalf("denklem doğrulanmalıydı: %v", err)
	}
	if !report.IsBalanced {
		t.Errorf("denklem dengede olmalıydı")
	}

	// Dengesiz durum testi: Kasa hesabına izinsiz ekleme yapılmış gibi test
	unbalancedCash, _ := cash.ApplyEntry(e1)
	badAccounts := []domain.Account{unbalancedCash, equity}
	report, err = domain.ValidateAccountingEquation(badAccounts, domain.USD)
	if err == nil || !isError(err, domain.ErrUnbalancedTransaction) {
		t.Errorf("dengesiz denklem için ErrUnbalancedTransaction bekleniyordu, alınan: %v", err)
	}
	if report.IsBalanced {
		t.Errorf("rapor dengesiz olarak işaretlenmeliydi")
	}
}
