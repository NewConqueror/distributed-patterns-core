package domain_test

import (
	"math"
	"testing"
	"testing/quick"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

// FuzzTransactionBalance, Go yerleşik fuzzing motoru (testing.F) kullanarak
// rastgele sayılarla muhasebe denkliği değişmezinin (sum(Debit) - sum(Credit) == 0)
// hiçbir koşulda delinmediğini kanıtlar.
func FuzzTransactionBalance(f *testing.F) {
	// Başlangıç tohumları (seed corpus)
	f.Add(int64(100), int64(200), int64(0))       // Tam dengeli
	f.Add(int64(5000), int64(5000), int64(1))     // 1 cent dengesiz
	f.Add(int64(1), int64(1), int64(-1))          // -1 cent dengesiz
	f.Add(int64(1000000), int64(500000), int64(0))// Büyük tutarlar
	f.Add(int64(0), int64(100), int64(0))         // Sıfır girdi
	f.Add(int64(-50), int64(50), int64(0))        // Negatif girdi

	f.Fuzz(func(t *testing.T, debit1, debit2, creditDelta int64) {
		// Yalnızca geçerli pozitif tutarlar ve taşma yapmayacak aralık için test
		if debit1 <= 0 || debit2 <= 0 {
			// Sıfır veya negatif tutarlarla Entry oluşturulması zaten domain seviyesinde engellenmeli
			m, err := domain.NewMoney(debit1, domain.USD)
			if err == nil {
				_, errEntry := domain.NewEntry("e1", "acc1", domain.DirectionDebit, m)
				if debit1 <= 0 && errEntry == nil {
					t.Fatalf("sıfır veya negatif tutarla girdi oluşturulabildi: %d", debit1)
				}
			}
			return
		}

		// Taşma koruması (int64 overflow)
		if debit1 > math.MaxInt64-debit2 {
			return
		}
		totalDebit := debit1 + debit2

		creditAmount := totalDebit + creditDelta
		if creditAmount <= 0 || creditAmount > math.MaxInt64 {
			return
		}

		mD1 := domain.MustNewMoney(debit1, domain.USD)
		mD2 := domain.MustNewMoney(debit2, domain.USD)
		mC1 := domain.MustNewMoney(creditAmount, domain.USD)

		e1, err1 := domain.NewEntry("d1", "acc-debit1", domain.DirectionDebit, mD1)
		e2, err2 := domain.NewEntry("d2", "acc-debit2", domain.DirectionDebit, mD2)
		e3, err3 := domain.NewEntry("c1", "acc-credit1", domain.DirectionCredit, mC1)

		if err1 != nil || err2 != nil || err3 != nil {
			t.Fatalf("geçerli girdiler oluşturulamadı: %v, %v, %v", err1, err2, err3)
		}

		tx, err := domain.NewTransaction("tx-fuzz", time.Now().UTC(), "Fuzz Test", []domain.Entry{e1, e2, e3})

		if creditDelta == 0 {
			// İşlem tam dengeli olmalı: sum(Debit) == sum(Credit)
			if err != nil {
				t.Fatalf("dengeli işlem reddedildi: debit1=%d, debit2=%d, credit=%d, err=%v", debit1, debit2, creditAmount, err)
			}
			if tx.TotalAmount().Amount() != totalDebit {
				t.Fatalf("işlem toplamı hatalı: beklenen %d, alınan %d", totalDebit, tx.TotalAmount().Amount())
			}

			// Ters işlem (reversal) testi:
			revTx, revErr := domain.CreateReversalTransaction(tx, "tx-fuzz-rev", time.Now().UTC(), "fuzz reversal")
			if revErr != nil {
				t.Fatalf("dengeli işlemin tersi oluşturulamadı: %v", revErr)
			}
			if revTx.TotalAmount().Amount() != tx.TotalAmount().Amount() {
				t.Fatalf("ters işlem toplamı uyuşmuyor")
			}
		} else {
			// İşlem dengesiz olmalı: sum(Debit) != sum(Credit)
			if err == nil {
				t.Fatalf("dengesiz işlem kabul edildi! debit=%d, credit=%d, fark=%d", totalDebit, creditAmount, creditDelta)
			}
			if !isError(err, domain.ErrUnbalancedTransaction) {
				t.Fatalf("ErrUnbalancedTransaction dışında bir hata alındı: %v", err)
			}
		}
	})
}

// TestQuickCheck_AccountingEquationInvariant, testing/quick paketi ile
// rastgele üretilen çoklu girdilerde muhasebe denkliğinin tam korunduğunu test eder.
func TestQuickCheck_AccountingEquationInvariant(t *testing.T) {
	property := func(amount1, amount2 uint32) bool {
		a1 := int64(amount1)%1000000 + 1
		a2 := int64(amount2)%1000000 + 1
		total := a1 + a2

		m1 := domain.MustNewMoney(a1, domain.USD)
		m2 := domain.MustNewMoney(a2, domain.USD)
		mTotal := domain.MustNewMoney(total, domain.USD)

		e1, _ := domain.NewEntry("e1", "cash", domain.DirectionDebit, m1)
		e2, _ := domain.NewEntry("e2", "bank", domain.DirectionDebit, m2)
		e3, _ := domain.NewEntry("e3", "equity", domain.DirectionCredit, mTotal)

		tx, err := domain.NewTransaction("tx-qc", time.Now().UTC(), "QuickCheck", []domain.Entry{e1, e2, e3})
		if err != nil {
			return false
		}

		// Reversal oluştur
		rev, err := domain.CreateReversalTransaction(tx, "tx-qc-rev", time.Now().UTC(), "QC Reversal")
		if err != nil {
			return false
		}

		// Net etki sıfır mı?
		cash, _ := domain.NewAccount("cash", "Kasa", domain.AccountTypeAsset, domain.USD)
		cash, _ = cash.ApplyEntry(tx.Entries()[0])
		cash, _ = cash.ApplyEntry(rev.Entries()[0])

		return cash.Balance().Amount() == 0
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("QuickCheck muhasebe denkliği ihlali buldu: %v", err)
	}
}
