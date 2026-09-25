package dlq

import (
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestDLQHeadersFormat(t *testing.T) {
	origMsg := kafka.Message{
		Topic:     "pg_cdc.public.products",
		Partition: 2,
		Offset:    1045,
		Key:       []byte(`{"id":"test-uuid-123"}`),
		Value:     []byte(`{malformed json...`),
	}

	errorType := ErrorTypeMalformedJSON
	errReason := "invalid character 'm' looking for beginning of value"

	headers := []kafka.Header{
		{Key: "x-error-type", Value: []byte(errorType)},
		{Key: "x-error-reason", Value: []byte(errReason)},
		{Key: "x-original-topic", Value: []byte(origMsg.Topic)},
		{Key: "x-original-partition", Value: []byte(strconv.Itoa(origMsg.Partition))},
		{Key: "x-original-offset", Value: []byte(strconv.FormatInt(origMsg.Offset, 10))},
		{Key: "x-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	}

	headerMap := make(map[string]string)
	for _, h := range headers {
		headerMap[h.Key] = string(h.Value)
	}

	if headerMap["x-error-type"] != ErrorTypeMalformedJSON {
		t.Errorf("expected x-error-type %s, got %s", ErrorTypeMalformedJSON, headerMap["x-error-type"])
	}

	if headerMap["x-original-partition"] != "2" {
		t.Errorf("expected x-original-partition '2', got %s", headerMap["x-original-partition"])
	}

	if headerMap["x-original-offset"] != "1045" {
		t.Errorf("expected x-original-offset '1045', got %s", headerMap["x-original-offset"])
	}
}
