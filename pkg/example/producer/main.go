package main

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"github.com/quarks-tech/amqpx"
	"github.com/quarks-tech/protoevent-go/example/gen/example/books/v1"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"

	"github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq"
)

func main() {
	client := amqpx.NewClient(&amqpx.Config{
		Address: "localhost:5672",
		AMQP: amqp.Config{
			Vhost: "/",
			SASL: []amqp.Authentication{
				&amqp.PlainAuth{
					Username: "guest",
					Password: "guest",
				},
			},
		},
		MinIdleConns: 4,
	})

	defer client.Close()

	sender := rabbitmq.NewSender(client)

	if err := sender.Setup(context.Background(), &books.EventbusServiceDesc); err != nil {
		log.Fatal(err)
	}

	publisher := eventbus.NewPublisher(sender, eventbus.WithDefaultPublishOptions(eventbus.WithEventContentType("application/cloudevents+json")))
	booksPublisher := books.NewEventPublisher(publisher)

	var eg errgroup.Group

	for i := int32(1); i <= 1; i++ {
		i := i
		eg.Go(func() error {
			fmt.Println("start publisher:", i)

			for c := int32(1); c <= 1; c++ {
				err := booksPublisher.PublishBookCreatedEvent(context.Background(), &books.BookCreatedEvent{
					Id: c * i,
				})
				if err != nil {
					fmt.Println(err)
				}
			}

			fmt.Println("stop publisher:", i)

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}
}
