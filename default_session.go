package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type DefaultSession struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// Assert at compile time that DefaultSession implements Session.
var _ = Session(&DefaultSession{})

func NewDefaultSession(address string) (*DefaultSession, error) {
	connection, err := amqp.Dial(address)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to broker")
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open a channel")
	}

	return &DefaultSession{connection, channel}, nil
}

func (s *DefaultSession) Connection() *amqp.Connection {
	return s.connection
}

func (s *DefaultSession) Channel() *amqp.Channel {
	return s.channel
}

func (s *DefaultSession) Close() error {
	if err := s.channel.Close(); err != nil {
		return err
	}
	if err := s.connection.Close(); err != nil {
		return err
	}
	return nil
}
