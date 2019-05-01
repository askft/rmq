package rmq

import (
	"github.com/streadway/amqp"
)

const (
	ExchangeDirect  = "direct"
	ExchangeTopic   = "topic"
	ExchangeFanout  = "fanout"
	ExchangeHeaders = "headers"
)

type Mailbox interface {
	Session
	Sender
	Receiver
}

type Session interface {
	Connection() *amqp.Connection
	Channel() *amqp.Channel
	Close() error
}

type Sender interface {
	Send(exchange, key string, message amqp.Publishing) error
}

type Receiver interface {
	Receive(exchange, queue, key string) (<-chan amqp.Delivery, error)
}
