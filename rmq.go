package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func connect(address string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to connect to broker")
	}
	// defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open a channel")
	}
	// defer ch.Close()

	return conn, ch, nil
}
