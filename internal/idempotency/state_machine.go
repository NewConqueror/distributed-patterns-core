package idempotency

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Storage, idempotency kayıtlarının kalıcı depolama sözleşmesidir.
type Storage interface {
	// CreateInFlight, anahtar yoksa IN_FLIGHT durumuyla yeni kayıt oluşturur.
	// Eğer anahtar zaten varsa, mevcut kaydı ve ErrKeyAlreadyExists hatasını döner.
	CreateInFlight(ctx context.Context, key, requestHash string, lockedAt time.Time) (*Record, error)

	// Resolve, işlemi tamamlanan kaydın durumunu RESOLVED yapar ve yanıtı kaydeder.
	Resolve(ctx context.Context, key string, responseCode int, responseBody []byte) error

	// Fail, başarısız olan kaydın durumunu FAILED yapar.
	Fail(ctx context.Context, key string, responseCode int, responseBody []byte) error

	// Get, belirli bir anahtara ait kaydı getirir.
	Get(ctx context.Context, key string) (*Record, error)
}

// Result, StateMachine tarafından üretilen çıktı nesnesidir.
type Result struct {
	ResponseCode int
	ResponseBody []byte
	IsCached     bool
}

// StateMachine, gelen istekleri Idempotency kurallarına göre koordine eden durum makinesidir.
type StateMachine struct {
	storage Storage
}

// NewStateMachine, yeni bir Idempotency durum makinesi başlatır.
func NewStateMachine(storage Storage) *StateMachine {
	return &StateMachine{
		storage: storage,
	}
}

// ExecuteFn, işlem yürütücü fonksiyon imzasıdır.
type ExecuteFn func(ctx context.Context) (responseCode int, responseBody []byte, err error)

// Process, gelen isteği 4 temel idempotency senaryosuna göre işler:
// Senaryo 1: Key ilk defa geliyorsa -> IN_FLIGHT durumuyla kaydet ve işlemi yürüt. Başarılıysa RESOLVED yap.
// Senaryo 2: Aynı key, aynı hash ile ve RESOLVED ise -> DB transaction'ına girmeden doğrudan kaydedilmiş response_body döndür.
// Senaryo 3: Aynı key, farklı hash ile geliyorsa -> 422 Unprocessable Entity (Key reuse hatası) fırlat.
// Senaryo 4: Aynı key, IN_FLIGHT durumundayken tekrar geliyorsa -> 409 Conflict ver.
func (sm *StateMachine) Process(ctx context.Context, key string, requestPayload []byte, execute ExecuteFn) (*Result, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, ErrInvalidIdempotencyKey
	}

	currentHash := ComputeRequestHash(requestPayload)
	now := time.Now().UTC()

	// 1. IN_FLIGHT olarak kaydetmeyi dene
	existing, err := sm.storage.CreateInFlight(ctx, key, currentHash, now)
	if err != nil && err != ErrKeyAlreadyExists {
		return nil, fmt.Errorf("idempotency anahtarı kaydedilemedi: %w", err)
	}

	// Eğer anahtar daha önceden varsa (existing != nil veya ErrKeyAlreadyExists)
	if existing != nil {
		// Senaryo 3: Farklı hash kontrolü (Key Reuse / Çakışma)
		if existing.RequestHash != currentHash {
			return nil, fmt.Errorf("%w: hash uyuşmazlığı (mevcut: %s, gelen: %s)",
				ErrIdempotencyKeyReuseWithDifferentPayload, existing.RequestHash, currentHash)
		}

		// Senaryo 4: Hala devam eden işlem (IN_FLIGHT)
		if existing.Status == StatusInFlight {
			return nil, ErrIdempotencyKeyInFlight
		}

		// Senaryo 2: Başarıyla sonuçlanmış işlem (RESOLVED)
		if existing.Status == StatusResolved {
			return &Result{
				ResponseCode: existing.ResponseCode,
				ResponseBody: existing.ResponseBody,
				IsCached:     true, // DB transaction'ına girmeden doğrudan önbellekten dönüldü
			}, nil
		}

		// Durum FAILED ise: Önceden başarısız olmuş bir işlemin cevabını dön
		if existing.Status == StatusFailed {
			return &Result{
				ResponseCode: existing.ResponseCode,
				ResponseBody: existing.ResponseBody,
				IsCached:     true,
			}, nil
		}
	}

	// Senaryo 1: Anahtar ilk defa kaydedildi, işlemi yürüt
	respCode, respBody, execErr := execute(ctx)
	if execErr != nil {
		// İşlem başarısız olduysa FAILED olarak güncelle
		_ = sm.storage.Fail(ctx, key, respCode, respBody)
		return nil, execErr
	}

	// İşlem başarılı olduysa RESOLVED olarak güncelle
	if resolveErr := sm.storage.Resolve(ctx, key, respCode, respBody); resolveErr != nil {
		return nil, fmt.Errorf("idempotency durumu RESOLVED yapılamadı: %w", resolveErr)
	}

	return &Result{
		ResponseCode: respCode,
		ResponseBody: respBody,
		IsCached:     false,
	}, nil
}
