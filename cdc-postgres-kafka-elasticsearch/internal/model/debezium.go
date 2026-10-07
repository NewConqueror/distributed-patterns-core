package model

const (
	OpRead   = "r" // Initial snapshot read
	OpCreate = "c" // Insert
	OpUpdate = "u" // Update
	OpDelete = "d" // Delete
)

// DebeziumKey represents the deserialized Kafka message key for PostgreSQL tables with PK
type DebeziumKey struct {
	ID string `json:"id"`
}

// DebeziumSource captures PostgreSQL WAL source metadata
type DebeziumSource struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  string `json:"snapshot"`
	DB        string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	TxID      int64  `json:"txId"`
	LSN       int64  `json:"lsn"`
}

// DebeziumEnvelope represents the top-level CDC message envelope
type DebeziumEnvelope[T any] struct {
	Before *T             `json:"before"`
	After  *T             `json:"after"`
	Source DebeziumSource `json:"source"`
	Op     string         `json:"op"`
	TsMs   int64          `json:"ts_ms"`
}

// IsDelete returns true if operation is delete
func (e *DebeziumEnvelope[T]) IsDelete() bool {
	return e.Op == OpDelete
}

// IsUpsert returns true if operation is create, update, or snapshot read
func (e *DebeziumEnvelope[T]) IsUpsert() bool {
	return e.Op == OpCreate || e.Op == OpUpdate || e.Op == OpRead
}
