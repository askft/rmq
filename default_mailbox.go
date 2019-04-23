package rmq

import (
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type DefaultMailbox struct {
	Session
}

// Assert at compile time that DefaultMailbox implements Mailbox.
var _ = Mailbox(&DefaultMailbox{})

func NewDefaultMailbox(address string) (*DefaultMailbox, error) {
	s, err := NewDefaultSession(address)
	if err != nil {
		return nil, errors.Wrap(err, "could not create session")
	}
	d := &DefaultMailbox{s}
	return d, nil
}

func (s *DefaultMailbox) Send(exchange, key string, data []byte) error {
	err := s.Channel().ExchangeDeclare(
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

	err = s.Channel().Publish(
		exchange, // exchange
		key,      // binding key
		false,    // mandatory
		false,    // immediate
		msg,      // message
	)
	return errors.Wrap(err, "failed to publish")
}

func (s *DefaultMailbox) Receive(exchange, queue, key string) (<-chan amqp.Delivery, error) {
	err := s.Channel().ExchangeDeclare(
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

	_, err = s.Channel().QueueDeclare(
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

	err = s.Channel().QueueBind(
		queue,    // queue name
		key,      // binding key
		exchange, // source exchange
		false,    // no wait
		nil,      // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "faied to bind queue")
	}

	return s.Channel().Consume(
		queue, // queue name
		"",    // consumer tag
		false, // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // args
	)
}
