package api

import (
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// Deps ...
type Deps struct {
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

// Handler ...
type Handler struct {
	randomizer             *rand.Rand
	monolithURL            *url.URL
	moviesServiceURL       *url.URL
	eventsServiceURL       *url.URL
	gradualMigration       bool
	moviesMigrationPercent int
}

// NewHandler ...
func NewHandler(deps Deps) *Handler {
	return &Handler{
		randomizer:             rand.New(rand.NewSource(time.Now().UnixNano())),
		monolithURL:            deps.MonolithURL,
		moviesServiceURL:       deps.MoviesServiceURL,
		eventsServiceURL:       deps.EventsServiceURL,
		gradualMigration:       deps.GradualMigration,
		moviesMigrationPercent: deps.MoviesMigrationPercent,
	}
}

func (h *Handler) InitRoutes() {
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/movies", h.moviesHandler)
	http.HandleFunc("/api/movies/health", h.moviesHealthHandler)
	http.HandleFunc("/api/events/", h.eventsHandler)
	http.HandleFunc("/api/users", h.monolithHandler)
	http.HandleFunc("/api/payments", h.monolithHandler)
	http.HandleFunc("/api/subscriptions", h.monolithHandler)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) moviesHealthHandler(w http.ResponseWriter, r *http.Request) {
	handler(w, r, h.moviesServiceURL)
}

func (h *Handler) eventsHandler(w http.ResponseWriter, r *http.Request) {
	handler(w, r, h.eventsServiceURL)
}

func (h *Handler) monolithHandler(w http.ResponseWriter, r *http.Request) {
	handler(w, r, h.monolithURL)
}

func (h *Handler) moviesHandler(w http.ResponseWriter, r *http.Request) {
	if !h.gradualMigration || h.randomizer.Intn(100) < h.moviesMigrationPercent {
		handler(w, r, h.moviesServiceURL)
		return
	}
	handler(w, r, h.monolithURL)
}

func handler(w http.ResponseWriter, r *http.Request, url *url.URL) {
	reverseProxy := httputil.NewSingleHostReverseProxy(url)
	reverseProxy.Director = func(req *http.Request) {
		req.URL.Scheme = url.Scheme
		req.URL.Host = url.Host
		req.URL.Path = r.URL.Path
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = url.Host
	}
	reverseProxy.ServeHTTP(w, r)
}
