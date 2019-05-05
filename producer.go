package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Producer struct {
	session      Session
	exchangeKind string
	exchangeName string
	key          string
}

func (p *Producer) Send(msg amqp.Publishing) error {
	return p.session.Channel().Publish(
		p.exchangeName, p.key, false, false, msg,
	)
}

func (p *Producer) SendJSON(msg MessageJSON) error {
	return p.Send(amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "application/json",
		CorrelationId: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		Body:          msg.Body,
	})
}

func (s *DefaultSession) MustCreateProducer(exchangeKind, exchangeName, key string) *Producer {
	producer, err := s.CreateProducer(exchangeKind, exchangeName, key)
	if err != nil {
		panic(errors.Wrap(err, "failed to create producer"))
	}
	return producer
}

func (s *DefaultSession) CreateProducer(exchangeKind, exchangeName, key string) (*Producer, error) {
	if err := s.Channel().ExchangeDeclare(
		exchangeName, // name
		exchangeKind, // kind
		false,        // durable?
		true,         // auto delete?
		false,        // internal?
		false,        // no wait?
		nil,          // args
	); err != nil {
		return nil, errors.Wrap(err, "failed to declare exchange")
	}
	return &Producer{
		s, exchangeKind, exchangeName, key,
	}, nil
}

// Create a one-off producer and send a message through it
func (s *DefaultSession) Send(exchangeKind, exchangeName, key string, msg amqp.Publishing) error {
	p, err := s.CreateProducer(exchangeKind, exchangeName, key)
	if err != nil {
		return err
	}
	return p.Send(msg)
}
