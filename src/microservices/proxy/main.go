package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/dkrasnykh/architecture-pro-cinemaabyss/src/microservices/proxy/internal/api"
)

func main() {
	var (
		deps api.Deps
		err  error
	)
	if deps.MonolithURL, err = url.Parse(getEnv("MONOLITH_URL", "http://localhost:8080")); err != nil {
		log.Fatal(err)
	}
	if deps.MoviesServiceURL, err = url.Parse(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")); err != nil {
		log.Fatal(err)
	}
	if deps.EventsServiceURL, err = url.Parse(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")); err != nil {
		log.Fatal(err)
	}
	if getEnv("GRADUAL_MIGRATION", "true") == "true" {
		deps.GradualMigration = true
	}
	if deps.MoviesMigrationPercent, err = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50")); err != nil {
		log.Fatal(err)
	}

	handlers := api.NewHandler(deps)
	handlers.InitRoutes()

	addr := fmt.Sprintf(":%s", getEnv("PORT", "8000"))
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
