package config

import (
	"github.com/kelseyhightower/envconfig"
)

type (
	// PulsarProducer holds the configuration for connecting to pulsar
	PulsarProducer struct {
		URL   string `envconfig:"URL" default:"pulsar+ssl://clump.raslup.snio.cloud:6651"`
		Topic string `envconfig:"TOPIC" default:"persistent://public/default/xchange-rates"`
		Name  string `envconfig:"NAME" default:"exchange-rate"`
		Token string `envconfig:"TOKEN"`
	}

	// PulsarConsumer holds the configuration for connecting to pulsar
	PulsarConsumer struct {
		URL              string `envconfig:"URL" default:"pulsar+ssl://clump.raslup.snio.cloud:6651"`
		Topic            string `envconfig:"TOPIC" default:"persistent://public/default/xchange-rates"`
		SubscriptionName string `envconfig:"SUBSCRIPTION_NAME" default:"xchange-rate-sub"`
		Token            string `envconfig:"TOKEN"`
	}
)

// NewPulsarProducer returns a PulsarProducer config
func NewPulsarProducer() *PulsarProducer {
	pp := &PulsarProducer{}

	envconfig.MustProcess("PULSAR_PRODUCER", pp)

	return pp
}

// NewPulsarProducer returns a PulsarProducer config
func NewPulsarConsumer() *PulsarConsumer {
	pc := &PulsarConsumer{}

	envconfig.MustProcess("PULSAR_CONSUMER", pc)

	return pc
}
