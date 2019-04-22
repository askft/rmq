package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Session struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func connect(address string) (*Session, error) {
	connection, err := amqp.Dial(address)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to broker")
	}
	// defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open a channel")
	}
	// defer channel.Close()

	return &Session{connection, channel}, nil
}
