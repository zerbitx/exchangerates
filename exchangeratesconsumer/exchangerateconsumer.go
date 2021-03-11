package exchangeratesconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zerbitx/currencies/config"
	"github.com/zerbitx/currencies/models"
	"log"
	"time"
)

type (
	exchangeRateConsumer struct {
		c      pulsar.Consumer
		closed chan struct{}
	}
)

// New returns a new consumer, "wired up" to a repository for record keeping
func New(pc *config.PulsarConsumer) (*exchangeRateConsumer, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            pc.URL,
		Authentication: pulsar.NewAuthenticationToken(pc.Token),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %s", err)
	}

	// create a consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       pc.Topic,
		SubscriptionName:            pc.SubscriptionName,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Type:                        pulsar.Exclusive,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &exchangeRateConsumer{
		c:      consumer,
		closed: make(chan struct{}),
	}, nil
}

// Consume consumes messages from pulsar indefinitely or until the context is canceled
// or an error occurs.
func (udc *exchangeRateConsumer) Consume(ctx context.Context) error {
	// Assumes all messages will be fully filled out, so we can reuse the container.
	ed := &models.ExchangeRate{}

	// defer, so this will be called even in the case of a panic
	defer func() {
		close(udc.closed)
	}()

	read := 0
	for {
		select {
		// If our context is cancelled, close the consumer and our closed signal
		case <-ctx.Done():
			udc.c.Close()
			return nil
		default:
			rctx, _ := context.WithTimeout(ctx, time.Second*2)
			msg, err := udc.c.Receive(rctx)
			if err != nil {
				// If its a context issue either try again, or if canceled, close the consumer
				if err == context.DeadlineExceeded || err == context.Canceled {
					continue
				}
				log.Fatalf("Failed to receive a message %v", err)
			}

			err = json.Unmarshal(msg.Payload(), ed)

			if err != nil {
				return err
			}

			read++
			fmt.Printf("🤑(%d) %v\n", read, ed)

			udc.c.Ack(msg)
		}
	}
}

// Allow a caller to know if/block until we've closed gracefully
func (udc *exchangeRateConsumer) WaitForClose() {
	<-udc.closed
}
