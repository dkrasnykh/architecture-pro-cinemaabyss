package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/cinemaabyss/microservices/events/internal/pkg/domain"
	"github.com/segmentio/kafka-go"
)

// Consumer ...
type Consumer struct {
	brokers        []string
	readMsgTimeout time.Duration
}

// New ...
func New(brokers []string, readMsgTimeout time.Duration) *Consumer {
	return &Consumer{
		brokers:        brokers,
		readMsgTimeout: readMsgTimeout,
	}
}

// Start ...
func (c *Consumer) Start(ctx context.Context, topic string) {
	time.Sleep(5 * time.Second)
	log.Printf("Consumer: listen topic %s", topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokers,
		Topic:          topic,
		GroupID:        fmt.Sprintf("%s.consumer_group", topic),
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	for {
		ctx, cancel := context.WithTimeout(ctx, c.readMsgTimeout)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("Consumer: ERROR failed to read %s topic message, error %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		var event domain.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Consumer: ERROR failed to unmarshal msg, topic %s, error %v", topic, err)
			continue
		}
		log.Printf("Consumer: handled event from topic %s, msg: %s", topic, string(msg.Value))
	}
}
