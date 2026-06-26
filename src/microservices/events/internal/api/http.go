package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/cinemaabyss/microservices/events/internal/pkg/domain"
)

// IProducer ...
type IProducer interface {
	SendMovieEvent(ctx context.Context, movie domain.Movie) (domain.EventResponse, error)
	SendUserEvent(ctx context.Context, user domain.User) (domain.EventResponse, error)
	SendPaymentEvent(ctx context.Context, payment domain.Payment) (domain.EventResponse, error)
}

// Handler ...
type Handler struct {
	producer IProducer
}

// New ...
func New(producer IProducer) *Handler {
	return &Handler{
		producer: producer,
	}
}

// InitRoutes ...
func (h *Handler) InitRoutes() {
	http.HandleFunc("/api/events/health", h.healthHandler)
	http.HandleFunc("/api/events/movie", h.movieHandler)
	http.HandleFunc("/api/events/user", h.userHandler)
	http.HandleFunc("/api/events/payment", h.paymentHandler)
}

func (h *Handler) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func (h *Handler) movieHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		return
	}
	var movie domain.Movie
	if err := json.NewDecoder(r.Body).Decode(&movie); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := h.producer.SendMovieEvent(context.Background(), movie)
	if err != nil {
		log.Printf("Error publishing movie event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) userHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		return
	}
	var user domain.User
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := h.producer.SendUserEvent(context.Background(), user)
	if err != nil {
		log.Printf("Error publishing user event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)

}

func (h *Handler) paymentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		return
	}
	var payment domain.Payment
	if err := json.NewDecoder(r.Body).Decode(&payment); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := h.producer.SendPaymentEvent(context.Background(), payment)
	if err != nil {
		log.Printf("Error publishing payment event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}
