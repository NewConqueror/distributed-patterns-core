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
	ID        int       `json:"id"`
	ProductID int       `json:"product_id"`
	UserID    string    `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type FastOrderResponse struct {
	Status        string `json:"status"`
	ReservationID string `json:"reservation_id"`
}
