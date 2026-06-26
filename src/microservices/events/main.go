package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cinemaabyss/microservices/events/internal/api"
	"github.com/cinemaabyss/microservices/events/internal/pkg/consumer"
	"github.com/cinemaabyss/microservices/events/internal/pkg/producer"
)

const (
	movieTopic   string = "events.movie.created"
	userTopic    string = "events.user.created"
	paymentTopic string = "events.payment.created"
)

func main() {
	appCtx := context.Background()

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")

	log.Printf("Starting events app, port %s", getEnv("PORT", "8082"))
	log.Printf("Kafka brokers: %s", strings.Join(brokers, ", "))

	consumer := consumer.New(brokers, time.Second*10)

	go consumer.Start(appCtx, movieTopic)
	go consumer.Start(appCtx, userTopic)
	go consumer.Start(appCtx, paymentTopic)

	producer := producer.New(brokers, time.Second*10, movieTopic, userTopic, paymentTopic)
	handler := api.New(producer)
	handler.InitRoutes()

	addr := fmt.Sprintf(":%s", getEnv("PORT", "8082"))
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
