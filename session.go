package rmq

import (
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Session struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewSession(address string) (*Session, error) {
	session, err := connect(address)
	if err != nil {
		return nil, errors.Wrap(err, "could not create session")
	}
	return session, nil
}

func (s *Session) Close() error {
	if err := s.channel.Close(); err != nil {
		return err
	}
	if err := s.connection.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Session) Send(exchange, key string, data []byte) error {
	err := s.channel.ExchangeDeclare(
		exchange, // name
		"direct", // kind
		true,     // durable
		false,    // auto delete
		false,    // internal
		false,    // no wait
		nil,      // args
	)
	if err != nil {
		return errors.Wrap(err, "failed to declare exchange")
	}

	// TODO - consider more options
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(data),
	}

	err = s.channel.Publish(
		exchange, // exchange
		key,      // binding key
		false,    // mandatory
		false,    // immediate
		msg,      // message
	)
	return errors.Wrap(err, "failed to publish")
}

func (s *Session) Receive(exchange, queue, key string) (<-chan amqp.Delivery, error) {
	err := s.channel.ExchangeDeclare(
		exchange, // name
		"direct", // kind
		true,     // durable
		false,    // auto delete
		false,    // internal
		false,    // no wait
		nil,      // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare exchange")
	}

	_, err = s.channel.QueueDeclare(
		queue, // queue name
		true,  // durable
		false, // auto delete
		false, // exclusive
		false, // no wait
		nil,   // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}

	err = s.channel.QueueBind(
		queue,    // queue name
		key,      // binding key
		exchange, // source exchange
		false,    // no wait
		nil,      // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "faied to bind queue")
	}

	return s.channel.Consume(
		queue, // queue name
		"",    // consumer tag
		false, // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // args
	)
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
