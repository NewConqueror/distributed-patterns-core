package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Status, bir idempotency anahtarının yaşam döngüsü durumunu ifade eder.
type Status string

const (
	StatusInFlight Status = "IN_FLIGHT" // İşlem yürütülüyor
	StatusResolved Status = "RESOLVED"  // İşlem başarıyla tamamlandı, sonuç önbellekte
	StatusFailed   Status = "FAILED"    // İşlem başarısızlıkla sonuçlandı
)

// Record, veritabanındaki idempotency_keys tablosunun nesne temsilidir.
type Record struct {
	Key          string
	RequestHash  string
	Status       Status
	ResponseCode int
	ResponseBody []byte
	LockedAt     time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ComputeRequestHash, gelen istek gövdesinin (payload) deterministik SHA-256 özetini üretir.
func ComputeRequestHash(payload []byte) string {
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
}
