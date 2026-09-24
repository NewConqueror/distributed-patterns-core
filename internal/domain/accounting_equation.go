package domain

import (
	"fmt"
)

// AccountingEquationReport, hesapların muhasebe denkliği durumunu ve toplamlarını özetleyen rapor nesnesidir.
type AccountingEquationReport struct {
	Currency        Currency
	TotalAssets     Money
	TotalLiabilities Money
	TotalEquity     Money
	TotalRevenue    Money
	TotalExpense    Money
	IsBalanced      bool
}

// ValidateAccountingEquation, verilen hesap listesinin temel muhasebe denkliğini sağlar:
// Genişletilmiş Muhasebe Denkliği:
// Assets + Expenses = Liabilities + Equity + Revenue
// Veya net özkaynak yaklaşımıyla:
// Assets = Liabilities + Equity + (Revenue - Expenses)
func ValidateAccountingEquation(accounts []Account, currency Currency) (*AccountingEquationReport, error) {
	if currency.IsZero() {
		return nil, ErrInvalidCurrency
	}

	zero, err := NewZeroMoney(currency)
	if err != nil {
		return nil, err
	}

	report := &AccountingEquationReport{
		Currency:         currency,
		TotalAssets:      zero,
		TotalLiabilities: zero,
		TotalEquity:      zero,
		TotalRevenue:     zero,
		TotalExpense:     zero,
		IsBalanced:       false,
	}

	for _, acc := range accounts {
		if !acc.Currency().Equals(currency) {
			return nil, fmt.Errorf("%w: hesap %s para birimi %s, beklenen %s",
				ErrCurrencyMismatch, acc.ID(), acc.Currency().Code(), currency.Code())
		}

		var addErr error
		switch acc.AccountType() {
		case AccountTypeAsset:
			report.TotalAssets, addErr = report.TotalAssets.Add(acc.Balance())
		case AccountTypeLiability:
			report.TotalLiabilities, addErr = report.TotalLiabilities.Add(acc.Balance())
		case AccountTypeEquity:
			report.TotalEquity, addErr = report.TotalEquity.Add(acc.Balance())
		case AccountTypeRevenue:
			report.TotalRevenue, addErr = report.TotalRevenue.Add(acc.Balance())
		case AccountTypeExpense:
			report.TotalExpense, addErr = report.TotalExpense.Add(acc.Balance())
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidAccountType, acc.AccountType())
		}
		if addErr != nil {
			return nil, fmt.Errorf("toplam hesaplanırken hata: %w", addErr)
		}
	}

	// Sol Taraf (Left Side): Assets + Expenses
	leftSide, err := report.TotalAssets.Add(report.TotalExpense)
	if err != nil {
		return nil, err
	}

	// Sağ Taraf (Right Side): Liabilities + Equity + Revenue
	rightSideTemp, err := report.TotalLiabilities.Add(report.TotalEquity)
	if err != nil {
		return nil, err
	}
	rightSide, err := rightSideTemp.Add(report.TotalRevenue)
	if err != nil {
		return nil, err
	}

	if !leftSide.Equals(rightSide) {
		report.IsBalanced = false
		return report, fmt.Errorf("%w: Sol (Varlık+Gider: %s) != Sağ (Yükümlülük+Özkaynak+Gelir: %s)",
			ErrUnbalancedTransaction, leftSide.String(), rightSide.String())
	}

	report.IsBalanced = true
	return report, nil
}
