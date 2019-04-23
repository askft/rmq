package rmq

import (
	"github.com/streadway/amqp"
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
	Send(exchange, key string, data []byte) error
}

type Receiver interface {
	Receive(exchange, queue, key string) (<-chan amqp.Delivery, error)
}
