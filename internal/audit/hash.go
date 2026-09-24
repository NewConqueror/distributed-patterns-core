package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// ZeroGenesisHash, önceden tanımlanmış 64 karakterlik sıfır seed hash sabitidir.
const ZeroGenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// GenesisHash, her hesap için ilk işlemde (entry #1) kullanılacak deterministik sabit seed hash üretir.
func GenesisHash(accountID string) string {
	payload := fmt.Sprintf("GENESIS_SEED:%s", accountID)
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}

// ComputeHash, belirtilen matematiksel formüle göre bir kaydın kriptografik hash'ini üretir:
// Hash_n = SHA256(Hash_{n-1} + TransactionID + Amount + Timestamp)
func ComputeHash(prevHash string, txID string, amount int64, timestamp time.Time) string {
	// Deterministik zaman gösterimi (UTC UnixNano)
	payload := fmt.Sprintf("%s|%s|%d|%d", prevHash, txID, amount, timestamp.UTC().UnixNano())
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}

// ComputeEntryHash, hesap hareketleri için yön (direction) ve hesap ID'sini de bağlayan genişletilmiş hash üretir.
func ComputeEntryHash(prevHash string, txID string, accountID string, direction string, amount int64, timestamp time.Time) string {
	payload := fmt.Sprintf("%s|%s|%s|%s|%d|%d", prevHash, txID, accountID, direction, amount, timestamp.UTC().UnixNano())
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}
