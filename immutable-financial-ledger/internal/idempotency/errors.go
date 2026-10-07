package idempotency

import "errors"

var (
	// ErrIdempotencyKeyReuseWithDifferentPayload (HTTP 422 Unprocessable Entity)
	// Aynı Idempotency-Key farklı bir istek gövdesi (request hash) ile tekrar gönderildiğinde fırlatılır.
	ErrIdempotencyKeyReuseWithDifferentPayload = errors.New("idempotency anahtarı farklı bir istek yükü ile tekrar kullanılamaz (422 Unprocessable Entity)")

	// ErrIdempotencyKeyInFlight (HTTP 409 Conflict / 425 Too Early)
	// Aynı anahtara sahip bir istek henüz işlemdeyken (IN_FLIGHT) tekrar geldiğinde fırlatılır.
	ErrIdempotencyKeyInFlight = errors.New("bu işlem şu anda başka bir istek tarafından yürütülmektedir (409 Conflict)")

	// ErrInvalidIdempotencyKey, anahtarın boş veya geçersiz formatta olması durumunda fırlatılır.
	ErrInvalidIdempotencyKey = errors.New("geçersiz idempotency anahtarı: anahtar boş veya geçersiz formatta")

	// ErrKeyNotFound, aranan anahtar veritabanında bulunamadığında döner.
	ErrKeyNotFound = errors.New("idempotency anahtarı bulunamadı")

	// ErrKeyAlreadyExists, anahtar zaten mevcut olduğunda döner.
	ErrKeyAlreadyExists = errors.New("idempotency anahtarı zaten mevcut")
)
