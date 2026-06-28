package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/cinemaabyss/microservices/events/internal/pkg/domain"
	"github.com/segmentio/kafka-go"
)

// Producer ...
type Producer struct {
	brokers         []string
	movieTopic      string
	userTopic       string
	paymentTopic    string
	writeMsgTimeout time.Duration
}

// New ...
func New(brokers []string, timeout time.Duration, movieTopic string, userTopic string, paymentTopic string) *Producer {
	return &Producer{
		brokers:         brokers,
		writeMsgTimeout: timeout,
		movieTopic:      movieTopic,
		userTopic:       userTopic,
		paymentTopic:    paymentTopic,
	}
}

// SendMovieEvent ...
func (p *Producer) SendMovieEvent(ctx context.Context, movie domain.Movie) (domain.EventResponse, error) {
	event := domain.Event{
		ID:        fmt.Sprintf("movie_%d_%s", movie.MovieID, movie.Action),
		Type:      "movie",
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   make(map[string]interface{}),
	}

	eventBytes, _ := json.Marshal(movie)
	json.Unmarshal(eventBytes, &event.Payload)

	partition, offset, err := p.sendMessage(ctx, p.movieTopic, event)
	if err != nil {
		log.Printf("Error publishing movie event: %v", err)
		return domain.EventResponse{}, nil
	}
	response := domain.EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	return response, nil
}

// SendUserEvent ...
func (p *Producer) SendUserEvent(ctx context.Context, user domain.User) (domain.EventResponse, error) {
	event := domain.Event{
		ID:        fmt.Sprintf("user_%d_%s", user.UserID, user.Action),
		Type:      "user",
		Timestamp: user.Timestamp,
		Payload:   make(map[string]interface{}),
	}
	eventBytes, _ := json.Marshal(user)
	json.Unmarshal(eventBytes, &event.Payload)

	partition, offset, err := p.sendMessage(ctx, p.userTopic, event)
	if err != nil {
		log.Printf("Error publishing user event: %v", err)
		return domain.EventResponse{}, err
	}
	resp := domain.EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	return resp, nil
}

// SendPaymentEvent ...
func (p *Producer) SendPaymentEvent(ctx context.Context, payment domain.Payment) (domain.EventResponse, error) {
	event := domain.Event{
		ID:        fmt.Sprintf("payment_%d_%s", payment.PaymentID, payment.Status),
		Type:      "payment",
		Timestamp: payment.Timestamp,
		Payload:   make(map[string]interface{}),
	}

	eventBytes, _ := json.Marshal(payment)
	json.Unmarshal(eventBytes, &event.Payload)

	partition, offset, err := p.sendMessage(ctx, p.paymentTopic, event)
	if err != nil {
		log.Printf("Error publishing payment event: %v", err)
		return domain.EventResponse{}, err
	}
	response := domain.EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	return response, nil
}

func (p *Producer) sendMessage(ctx context.Context, topic string, event domain.Event) (int, int64, error) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  p.brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    false,
	})
	defer writer.Close()

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	msg := kafka.Message{
		Key:   []byte(event.ID),
		Value: eventBytes,
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		return 0, 0, err
	}

	log.Printf("Published event to topic %s: %s", topic, event.ID)

	return 0, 0, nil
}
