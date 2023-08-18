package main

import (
	"context"
	"errors"
	"fmt"
	nats "github.com/nats-io/nats.go"
	"log"
	"time"
)

func main() {
	err := runConsumers()
	if err != nil {
		log.Fatal("run consumers", err)
	}
}

func runConsumers() error {
	nc, err := nats.Connect("nats://localhost:4222,nats://localhost:4223,nats://localhost:4224")
	if err != nil {
		return fmt.Errorf("nats connect %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	streamName := "stream_name"
	subject := "test_subject_name"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:              streamName,
		Subjects:          []string{subject},
		Retention:         nats.InterestPolicy, // Don't store the acked messages.
		Discard:           nats.DiscardOld,     // When the queue is full, discard old messages.
		MaxAge:            7 * 24 * time.Hour,  // 7 days
		Storage:           nats.FileStorage,
		MaxMsgsPerSubject: 100_000_000,
		MaxMsgSize:        4 << 20, // 4 MB
		NoAck:             false,
	}, nats.Context(context.Background()))
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("add stream: %w", err)
	}

	consumerGroupName := "consumer_group_name"
	_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerGroupName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       10 * time.Second,
		MaxAckPending: -1,
	}, nats.Context(context.Background()))
	if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		return fmt.Errorf("add consumer: %w", err)
	}

	jetStreamSubscriber, err := js.PullSubscribe(
		subject,
		consumerGroupName,
		nats.ManualAck(),
		nats.Bind(streamName, consumerGroupName),
		nats.Context(context.Background()), // The context must be active until app is running.
	)
	if err != nil {
		return fmt.Errorf("pull subscribe: %w", err)
	}

	totalReceived := int32(0)
	for {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			msgs, fetchErr := jetStreamSubscriber.Fetch(1, nats.Context(ctx))
			if fetchErr != nil {
				if errors.Is(fetchErr, context.DeadlineExceeded) {
					return
				}

				log.Printf("sub fetch err: %s\n", fetchErr)
				return
			}

			for _, msg := range msgs {
				nakErr := msg.Nak()
				if nakErr != nil {
					log.Printf("sub nak err: %s\n", nakErr)
				}
				totalReceived++
				log.Printf("total received: %d\n", totalReceived)
			}
		}()
	}

	return nil
}
