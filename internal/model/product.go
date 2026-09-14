package model

type Product struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Stock   int    `json:"stock"`
	Version int    `json:"version"`
}

type ResetStockRequest struct {
	Stock int `json:"stock"`
}
