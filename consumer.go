package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func NewConsumer(address string) (*Consumer, error) {
	session, err := connect(address)
	if err != nil {
		return nil, errors.Wrap(err, "could not create consumer")
	}
	return &Consumer{
		session,
	}, nil
}

type Consumer struct {
	*Session
}

func (c *Consumer) Receive(exchange, queue, key string) (<-chan amqp.Delivery, error) {
	err := c.channel.ExchangeDeclare(
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

	_, err = c.channel.QueueDeclare(
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

	err = c.channel.QueueBind(
		queue,    // queue name
		key,      // binding key
		exchange, // source exchange
		false,    // no wait
		nil,      // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "faied to bind queue")
	}

	return c.channel.Consume(
		queue, // queue name
		"",    // consumer tag
		false, // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // args
	)
}
