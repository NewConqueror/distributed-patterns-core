package ingestion

import (
	"sync/atomic"
)

// Metrics holds atomic counters for observability in the ingestion engine.
type Metrics struct {
	ActiveStreams      atomic.Int64
	TotalReceived      atomic.Uint64
	TotalProcessed     atomic.Uint64
	TotalDroppedOldest atomic.Uint64
	TotalDroppedNewest atomic.Uint64
	streamSeq          atomic.Uint64
}

// NewMetrics initializes an ingestion metrics container.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// NextStreamID generates a unique identifier for each incoming gRPC stream connection.
func (m *Metrics) NextStreamID() uint64 {
	return m.streamSeq.Add(1)
}

// Snapshot provides a point-in-time view of engine throughput & stream lifecycle.
type Snapshot struct {
	ActiveStreams      int64  `json:"active_streams"`
	TotalReceived      uint64 `json:"total_received"`
	TotalProcessed     uint64 `json:"total_processed"`
	TotalDroppedOldest uint64 `json:"total_dropped_oldest"`
	TotalDroppedNewest uint64 `json:"total_dropped_newest"`
}

// Snapshot captures the current metric counters.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		ActiveStreams:      m.ActiveStreams.Load(),
		TotalReceived:      m.TotalReceived.Load(),
		TotalProcessed:     m.TotalProcessed.Load(),
		TotalDroppedOldest: m.TotalDroppedOldest.Load(),
		TotalDroppedNewest: m.TotalDroppedNewest.Load(),
	}
}
