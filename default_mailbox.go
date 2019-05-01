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
	return &DefaultMailbox{s}, nil
}

func (s *DefaultMailbox) SendJSON(exchange, key string, body []byte) error {
	return s.Send(
		exchange,
		key,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         body,
		},
	)
}

func (s *DefaultMailbox) Send(exchange, key string, message amqp.Publishing) error {
	if err := s.Channel().ExchangeDeclare(
		exchange,       // name
		ExchangeDirect, // kind
		true,           // durable?
		false,          // auto delete?
		false,          // internal?
		false,          // no wait?
		nil,            // args
	); err != nil {
		return errors.Wrap(err, "failed to declare exchange")
	}

	err := s.Channel().Publish(
		exchange, // exchange
		key,      // binding key
		false,    // mandatory?
		false,    // immediate?
		message,  // message
	)
	return errors.Wrap(err, "failed to publish")
}

// TODO also let user specify consumer tag?
func (s *DefaultMailbox) Receive(exchange, queue, key string) (<-chan amqp.Delivery, error) {

	isDurable := true

	if err := s.Channel().ExchangeDeclare(
		exchange,       // name
		ExchangeDirect, // kind
		isDurable,      // durable?
		false,          // auto delete?
		false,          // internal?
		false,          // no wait?
		nil,            // args
	); err != nil {
		return nil, errors.Wrap(err, "failed to declare exchange")
	}

	if _, err := s.Channel().QueueDeclare(
		queue,     // queue name
		isDurable, // durable
		false,     // auto delete?
		false,     // exclusive?
		false,     // no wait?
		nil,       // args
	); err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}

	if err := s.Channel().QueueBind(
		queue,    // queue name
		key,      // binding key
		exchange, // source exchange
		false,    // no wait?
		nil,      // args
	); err != nil {
		return nil, errors.Wrap(err, "faied to bind queue")
	}

	return s.Channel().Consume(
		queue, // queue name
		"",    // consumer tag
		false, // auto ack?
		false, // exclusive?
		false, // no local?
		false, // no wait?
		nil,   // args
	)
}
