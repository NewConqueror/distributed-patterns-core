package test

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"google.golang.org/protobuf/proto"
)

// JSONLocationPing mirrors LocationPing fields for direct JSON payload size & serialization comparison.
type JSONLocationPing struct {
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Bearing   float32 `json:"bearing"`
	Speed     float32 `json:"speed"`
	Timestamp int64   `json:"timestamp"`
}

func sampleData() (telemetryv1.LocationPing, JSONLocationPing) {
	driverID := "driver-ist-8841"
	lat := 41.0082376
	lon := 28.9783589
	bearing := float32(184.5)
	speed := float32(62.4)
	ts := time.Now().UnixMilli()

	protoPing := telemetryv1.LocationPing{
		DriverId:  driverID,
		Latitude:  lat,
		Longitude: lon,
		Bearing:   bearing,
		Speed:     speed,
		Timestamp: ts,
	}

	jsonPing := JSONLocationPing{
		DriverID:  driverID,
		Latitude:  lat,
		Longitude: lon,
		Bearing:   bearing,
		Speed:     speed,
		Timestamp: ts,
	}

	return protoPing, jsonPing
}

// TestSinglePingPayloadComparison verifies payload size differences between Protobuf and JSON for a single telemetry ping.
func TestSinglePingPayloadComparison(t *testing.T) {
	protoMsg, jsonMsg := sampleData()

	protoBytes, err := proto.Marshal(&protoMsg)
	if err != nil {
		t.Fatalf("Failed to marshal Protobuf ping: %v", err)
	}

	jsonBytes, err := json.Marshal(&jsonMsg)
	if err != nil {
		t.Fatalf("Failed to marshal JSON ping: %v", err)
	}

	protoSize := len(protoBytes)
	jsonSize := len(jsonBytes)
	bytesSaved := jsonSize - protoSize
	reductionPct := (float64(bytesSaved) / float64(jsonSize)) * 100

	t.Logf("\n=======================================================\n"+
		"   PAYLOAD SIZE COMPARISON (SINGLE TELEMETRY PING)\n"+
		"=======================================================\n"+
		"Protobuf Wire Size : %4d bytes\n"+
		"JSON Wire Size     : %4d bytes\n"+
		"Bandwidth Saved    : %4d bytes (%.2f%% smaller)\n"+
		"Raw Protobuf Hex   : %x\n"+
		"Raw JSON String    : %s\n"+
		"=======================================================",
		protoSize, jsonSize, bytesSaved, reductionPct, protoBytes, string(jsonBytes))

	if protoSize >= jsonSize {
		t.Errorf("Expected Protobuf (%d bytes) to be smaller than JSON (%d bytes)", protoSize, jsonSize)
	}

	// Verify Protobuf unmarshal round-trip accuracy
	var roundtripProto telemetryv1.LocationPing
	if err := proto.Unmarshal(protoBytes, &roundtripProto); err != nil {
		t.Fatalf("Failed to unmarshal Protobuf: %v", err)
	}
	if roundtripProto.GetDriverId() != protoMsg.DriverId ||
		math.Abs(roundtripProto.GetLatitude()-protoMsg.Latitude) > 1e-7 ||
		math.Abs(roundtripProto.GetLongitude()-protoMsg.Longitude) > 1e-7 {
		t.Errorf("Protobuf roundtrip corrupted data: %+v != %+v", roundtripProto, protoMsg)
	}
}

// TestBatchPayloadComparison evaluates payload size savings for a fleet batch of 1,000 pings.
func TestBatchPayloadComparison(t *testing.T) {
	const batchSize = 1000
	protoPings := make([]*telemetryv1.LocationPing, batchSize)
	jsonPings := make([]JSONLocationPing, batchSize)

	now := time.Now().UnixMilli()
	for i := 0; i < batchSize; i++ {
		driverID := fmt.Sprintf("driver-ist-%04d", i)
		lat := 41.000000 + float64(i)*0.0001
		lon := 28.900000 + float64(i)*0.0001
		bearing := float32(i % 360)
		speed := float32(40 + (i % 50))
		ts := now + int64(i*100)

		protoPings[i] = &telemetryv1.LocationPing{
			DriverId:  driverID,
			Latitude:  lat,
			Longitude: lon,
			Bearing:   bearing,
			Speed:     speed,
			Timestamp: ts,
		}
		jsonPings[i] = JSONLocationPing{
			DriverID:  driverID,
			Latitude:  lat,
			Longitude: lon,
			Bearing:   bearing,
			Speed:     speed,
			Timestamp: ts,
		}
	}

	// Calculate individual wire stream size for Protobuf
	var totalProtoBytes int
	for _, p := range protoPings {
		b, err := proto.Marshal(p)
		if err != nil {
			t.Fatalf("Failed to marshal proto item: %v", err)
		}
		totalProtoBytes += len(b)
	}

	// Calculate JSON array payload
	jsonBytes, err := json.Marshal(jsonPings)
	if err != nil {
		t.Fatalf("Failed to marshal json slice: %v", err)
	}
	totalJSONBytes := len(jsonBytes)

	savedBytes := totalJSONBytes - totalProtoBytes
	pctSaved := (float64(savedBytes) / float64(totalJSONBytes)) * 100

	t.Logf("\n=======================================================\n"+
		"   BATCH PAYLOAD COMPARISON (%d CONCURRENT PINGS)\n"+
		"=======================================================\n"+
		"Total Protobuf Size : %7d bytes (%.2f KB)\n"+
		"Total JSON Size     : %7d bytes (%.2f KB)\n"+
		"Bandwidth Saved     : %7d bytes (%.2f KB - %.2f%% reduction)\n"+
		"Network Egress/sec @ 10,000 pings/s:\n"+
		"   Protobuf: %.2f MB/s\n"+
		"   JSON:     %.2f MB/s\n"+
		"=======================================================",
		batchSize,
		totalProtoBytes, float64(totalProtoBytes)/1024,
		totalJSONBytes, float64(totalJSONBytes)/1024,
		savedBytes, float64(savedBytes)/1024, pctSaved,
		(float64(totalProtoBytes)/float64(batchSize)*10000)/(1024*1024),
		(float64(totalJSONBytes)/float64(batchSize)*10000)/(1024*1024),
	)
}

// -------------------------------------------------------------
// BENCHMARKS: Throughput and Allocation Comparison
// -------------------------------------------------------------

func BenchmarkProtobufMarshal(b *testing.B) {
	protoMsg, _ := sampleData()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := proto.Marshal(&protoMsg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONMarshal(b *testing.B) {
	_, jsonMsg := sampleData()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(&jsonMsg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtobufUnmarshal(b *testing.B) {
	protoMsg, _ := sampleData()
	bytes, _ := proto.Marshal(&protoMsg)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out telemetryv1.LocationPing
		if err := proto.Unmarshal(bytes, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
	_, jsonMsg := sampleData()
	bytes, _ := json.Marshal(&jsonMsg)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out JSONLocationPing
		if err := json.Unmarshal(bytes, &out); err != nil {
			b.Fatal(err)
		}
	}
}
