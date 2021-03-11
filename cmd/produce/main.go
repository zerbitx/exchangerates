package main

import (
	"context"
	"fmt"
	"github.com/zerbitx/currencies/config"
	"github.com/zerbitx/currencies/exchangeratesproducer"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	pp := config.NewPulsarProducer()
	p, err := exchangeratesproducer.New(pp)

	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFunc := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)
	defer cancelFunc()

	fmt.Println("💥", p.Produce(ctx))
}
