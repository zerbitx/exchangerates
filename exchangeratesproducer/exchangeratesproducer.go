package exchangeratesproducer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zerbitx/currencies/models"
	"net/http"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zerbitx/currencies/config"
)

type (
	ExchangeRateResponse struct {
		Data struct {
			Rates map[string]string `json:"rates"`
		} `json:"data"`
	}

	Currency struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	}

	CurrenciesResponse struct {
		Data []Currency
	}

	exchangeRateProducer struct {
		p pulsar.Producer
	}
)

var httpClient = http.Client{
	Timeout: time.Second * 10,
}

// New returns a new producer, "wired up" to a repository for record keeping
func New(pc *config.PulsarProducer) (*exchangeRateProducer, error) {
	tokenAuth := pulsar.NewAuthenticationToken(pc.Token)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            pc.URL,
		Authentication: tokenAuth,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %s", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: pc.Topic,
		Name:  pc.Name,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &exchangeRateProducer{
		p: producer,
	}, nil
}

func getCurrencies() (map[string]Currency, error) {
	req, err := http.NewRequest(http.MethodGet, "https://api.coinbase.com/v2/exchangerates", nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create exchange rate request: %w", err)
	}

	res, err := httpClient.Do(req)

	if err != nil {
		return nil, fmt.Errorf("failed to get exchangerates: %w", err)
	}

	cr := &CurrenciesResponse{}

	err = json.NewDecoder(res.Body).Decode(cr)

	if err != nil {
		return nil, fmt.Errorf("failed to decode exchangerates response: %w", err)
	}

	currencies := map[string]Currency{}
	for i := range cr.Data {
		cur := cr.Data[i]

		currencies[cur.ID] = cur
	}

	return currencies, nil
}

// Produce consumes messages from pulsar indefinitely or until the context is canceled
// or an error occurs.
func (erp *exchangeRateProducer) Produce(ctx context.Context) error {
	erReq, err := http.NewRequest(http.MethodGet, "https://api.coinbase.com/v2/exchange-rates", nil)

	if err != nil {
		return fmt.Errorf("failed to create exchange rate request: %w", err)
	}

	currencies, err := getCurrencies()

	if err != nil {
		return err
	}

	sent := 0
	wait := time.After(time.Second * 10)
	for {
		select {
		case <-wait:
			fmt.Printf("Sent: %d messages\n", sent)
			res, err := httpClient.Do(erReq)

			if err != nil {
				return fmt.Errorf("failed to make request: %w", err)
			}

			resRates := &ExchangeRateResponse{}
			err = json.NewDecoder(res.Body).Decode(&resRates)

			if err != nil {
				return fmt.Errorf("failed to decode response body: %w", err)
			}

			at := time.Now().Unix()
			for curID, rateStr := range resRates.Data.Rates {
				if _, ok := currencies[curID]; ok {
					rateFloatValue, err := strconv.ParseFloat(rateStr, 64)

					if err != nil {
						return fmt.Errorf("failed to ")
					}

					rate := &models.ExchangeRate{
						Name: currencies[curID].Name,
						ID:   curID,
						Rate: rateFloatValue,
						At:   at,
					}

					rb, err := json.Marshal(rate)

					if err != nil {
						return fmt.Errorf("failed to encode payload: %w", err)
					}

					_, err = erp.p.Send(ctx, &pulsar.ProducerMessage{
						Payload: rb,
					})

					if err != nil {
						return fmt.Errorf("failed to send: %s %w", string(rb))
					}
					sent++
					wait = time.After(time.Second * 10)
				}
			}
		case <-ctx.Done():
			erp.p.Close()
			return nil
		}
	}
}
