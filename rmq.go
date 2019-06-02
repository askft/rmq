package rmq

import (
	"log"

	"github.com/streadway/amqp"
)

const (
	ExchangeDirect  = "direct"
	ExchangeTopic   = "topic"
	ExchangeFanout  = "fanout"
	ExchangeHeaders = "headers"
)

type MessageHandler func(amqp.Delivery)

type Middleware func(MessageHandler) MessageHandler

func (h MessageHandler) With(mw Middleware) MessageHandler {
	return mw(h)
}

func Chain(middlewares []Middleware, handler MessageHandler) MessageHandler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

type Handler interface {
	Handle(d amqp.Delivery)
	DeliveryChan() <-chan amqp.Delivery
}

func RunAsync(handler Handler, mws ...Middleware) {
	go RunSync(handler, mws...)
}

func RunSync(handler Handler, mws ...Middleware) {
	handle := Chain(mws, handler.Handle)
	for d := range handler.DeliveryChan() {
		handle(d)
		if err := d.Ack(false); err != nil {
			log.Fatal(err)
		}
	}
}
