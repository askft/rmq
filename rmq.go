package rmq

import "github.com/streadway/amqp"

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
