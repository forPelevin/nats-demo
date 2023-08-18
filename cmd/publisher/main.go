package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"strconv"
)

func main() {
	err := runPublishers()
	if err != nil {
		log.Fatal("run publishers", err)
	}
}

func runPublishers() error {
	nc, err := nats.Connect("nats://localhost:4222,nats://localhost:4223,nats://localhost:4224")
	if err != nil {
		return fmt.Errorf("nats connect %w", err)
	}

	jetStream, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	publishedTotal := 0
	for i := 0; i < 30000; i++ {
		subject := "test_subject_name"
		_, err = jetStream.PublishAsync(subject, []byte(strconv.Itoa(i)))
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}

		publishedTotal++
	}
	fmt.Printf("published %d messages\n", publishedTotal)

	return nil
}
