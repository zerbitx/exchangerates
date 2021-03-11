package main

import (
	"context"
	"fmt"
	"github.com/zerbitx/currencies/config"
	"github.com/zerbitx/currencies/exchangeratesconsumer"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	pc := config.NewPulsarConsumer()
	p, err := exchangeratesconsumer.New(pc)

	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFunc := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)
	defer cancelFunc()

	fmt.Println("💥", p.Consume(ctx))

	p.WaitForClose()
}
