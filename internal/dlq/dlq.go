package dlq

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	ErrorTypeMalformedJSON    = "MALFORMED_JSON"
	ErrorTypeTransformFailed  = "TRANSFORM_FAILED"
	ErrorTypeMappingConflict  = "MAPPING_CONFLICT"
	ErrorTypeValidationFailed = "VALIDATION_FAILED"
)

type Producer interface {
	Publish(ctx context.Context, originalMsg kafka.Message, errorType string, errReason string) error
	Close() error
}

type KafkaDLQProducer struct {
	writer   *kafka.Writer
	dlqTopic string
}

func NewKafkaDLQProducer(brokers []string, dlqTopic string) *KafkaDLQProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll, // Strong durability for dead letters
		Async:        false,
	}

	return &KafkaDLQProducer{
		writer:   writer,
		dlqTopic: dlqTopic,
	}
}

// Publish routes a poison pill / permanently failed message to the DLQ topic with diagnostic headers
func (p *KafkaDLQProducer) Publish(ctx context.Context, originalMsg kafka.Message, errorType string, errReason string) error {
	headers := []kafka.Header{
		{Key: "x-error-type", Value: []byte(errorType)},
		{Key: "x-error-reason", Value: []byte(errReason)},
		{Key: "x-original-topic", Value: []byte(originalMsg.Topic)},
		{Key: "x-original-partition", Value: []byte(strconv.Itoa(originalMsg.Partition))},
		{Key: "x-original-offset", Value: []byte(strconv.FormatInt(originalMsg.Offset, 10))},
		{Key: "x-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	}

	dlqMsg := kafka.Message{
		Key:     originalMsg.Key,
		Value:   originalMsg.Value,
		Headers: headers,
		Time:    time.Now().UTC(),
	}

	start := time.Now()
	if err := p.writer.WriteMessages(ctx, dlqMsg); err != nil {
		return fmt.Errorf("failed to write message to DLQ topic %s: %w", p.dlqTopic, err)
	}

	log.Printf("[DLQ Forwarded] Diverted poison message to '%s' (Partition: %d, Offset: %d, ErrorType: %s, Reason: %s) in %v",
		p.dlqTopic, originalMsg.Partition, originalMsg.Offset, errorType, errReason, time.Since(start))

	return nil
}

func (p *KafkaDLQProducer) Close() error {
	return p.writer.Close()
}
