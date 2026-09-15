package model

import "time"

type OrderRequest struct {
	ProductID int    `json:"product_id"`
	UserID    string `json:"user_id"`
}

type OrderResponse struct {
	OrderID   int    `json:"order_id"`
	ProductID int    `json:"product_id"`
	UserID    string `json:"user_id"`
	Message   string `json:"message"`
}

type Order struct {
	ID            int       `json:"id"`
	ReservationID string    `json:"reservation_id,omitempty"`
	ProductID     int       `json:"product_id"`
	UserID        string    `json:"user_id"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type FastOrderResponse struct {
	Status        string `json:"status"`
	ReservationID string `json:"reservation_id"`
	TTLSeconds    int    `json:"ttl_seconds,omitempty"`
}

type PayOrderRequest struct {
	ReservationID string `json:"reservation_id"`
}

type PayOrderResponse struct {
	ReservationID string `json:"reservation_id"`
	Status        string `json:"status"`
	Message       string `json:"message"`
}
