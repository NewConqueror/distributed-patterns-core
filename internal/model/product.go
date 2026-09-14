package model

type Product struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Stock int    `json:"stock"`
}

type ResetStockRequest struct {
	Stock int `json:"stock"`
}
