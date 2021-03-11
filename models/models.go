package models

type (
	// ExchangeRate is the expected PulsarProducer usage message shape
	ExchangeRate struct {
		Name string  `json:"name"`
		ID   string  `json:"id"`
		Rate float64 `json:"rate"`
		At   int64   `json:"at"`
	}
)
