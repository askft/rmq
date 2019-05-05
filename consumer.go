package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type ConsumerStream struct {
	C       <-chan amqp.Delivery
	session Session
}

// panics if error
func (s *DefaultSession) MustCreateConsumerStream(exchangeKind, exchangeName, queue, key string) *ConsumerStream {
	stream, err := s.CreateConsumerStream(exchangeKind, exchangeName, queue, key)
	if err != nil {
		panic(errors.Wrap(err, "failed to create stream"))
	}
	return stream
}

// TODO also let user specify consumer tag?
func (s *DefaultSession) CreateConsumerStream(exchangeKind, exchangeName, queue, key string) (*ConsumerStream, error) {
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

	if _, err := s.Channel().QueueDeclare(
		queue, // queue name
		false, // durable
		true,  // auto delete?
		false, // exclusive?
		false, // no wait?
		nil,   // args
	); err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}

	if err := s.Channel().QueueBind(
		queue,        // queue name
		key,          // binding key
		exchangeName, // source exchange
		false,        // no wait?
		nil,          // args
	); err != nil {
		return nil, errors.Wrap(err, "faied to bind queue")
	}

	c, err := s.Channel().Consume(
		queue, // queue name
		"",    // consumer tag
		false, // auto ack?
		false, // exclusive?
		false, // no local?
		false, // no wait?
		nil,   // args
	)
	if err != nil {
		return nil, err
	}
	return &ConsumerStream{c, s}, nil
}
