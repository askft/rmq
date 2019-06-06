package main

import (
	"log"
	"os"
	"time"

	"github.com/lithammer/shortuuid"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/askft/rmq"
)

var (
	exchange = "hello-exchange"
	queue    = "hello-queue"
	command  = "hello-command"
	event    = "hello-event"
)

func main() {

	rmqURL := os.Getenv("RMQ_URL")

	// Establish one connection for consuming and one for producing.
	cc := rmq.MustNewConsumerConnection(rmqURL)
	pc := rmq.MustNewProducerConnection(rmqURL)

	// Create and run a HelloHandler.
	// The RMQ connections are being explicitly injected here.
	go rmq.RunSync(NewHelloHandler(cc, pc))

	// Send a message that hopefully reaches the HelloHandler.
	if err := pc.Send(
		rmq.ExchangeDirect,
		exchange,
		command,
		rmq.MessageJSON{
			CorrelationID: shortuuid.New(),
			ReplyTo:       event,
			Body:          []byte("Hello from main."),
		}.ToAMQP(),
	); err != nil {
		log.Panic(errors.Wrap(err, "could not send message"))
	}

	// Create a consumer just so we can wait
	// for a response from the HelloHandler.
	consumer := cc.MustCreateStream(
		rmq.ExchangeDirect,
		exchange,
		"otherqueue",
		event,
	)
	defer consumer.Channel.Close()

	select {
	case d := <-consumer.C:
		log.Printf("Received message %q\n", string(d.Body))
		if err := d.Ack(false); err != nil {
			log.Panic(errors.Wrap(err, "could not ack"))
		}
	case <-time.After(2 * time.Second):
		log.Panic("request timed out")
	}

	select {}
}

type HelloHandler struct {
	consumer *rmq.Consumer
	producer *rmq.Producer
}

func NewHelloHandler(cc *rmq.ConsumerConnection, pc *rmq.ProducerConnection) *HelloHandler {

	consumer := cc.MustCreateStream(
		rmq.ExchangeDirect,
		exchange,
		queue,
		command,
	)

	producer := pc.MustCreateStream(
		rmq.ExchangeDirect,
		exchange,
		event,
	)

	return &HelloHandler{consumer, producer}
}

func (h *HelloHandler) DeliveryChan() <-chan amqp.Delivery {
	return h.consumer.C
}

func (h *HelloHandler) Handle(d amqp.Delivery) {
	log.Printf("Received message %q\n", string(d.Body))

	if err := h.producer.SendWithKey(
		d.ReplyTo,
		rmq.MessageJSON{
			CorrelationID: d.CorrelationId,
			Body:          []byte("Hello from HelloHandler!"),
		}.ToAMQP(),
	); err != nil {
		log.Panic(errors.Wrap(err, "could not send message"))
	}
}
