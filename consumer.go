package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func NewConsumer(address, exchange string) (*Consumer, error) {
	session, err := connect(address)
	if err != nil {
		return nil, errors.Wrap(err, "could not create consumer")
	}
	return &Consumer{
		session,
		exchange,
	}, nil
}

type Consumer struct {
	*Session
	exchange string
}

func (c *Consumer) Receive(queueName, key string) (<-chan amqp.Delivery, error) {
	err := c.channel.ExchangeDeclare(
		c.exchange, // name
		"direct",   // kind
		true,       // durable
		false,      // auto delete
		false,      // internal
		false,      // no wait
		nil,        // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare exchange")
	}

	_, err = c.channel.QueueDeclare(
		queueName, // queue name
		true,      // durable
		false,     // auto delete
		false,     // exclusive
		false,     // no wait
		nil,       // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}

	err = c.channel.QueueBind(
		queueName,  // queue name
		key,        // binding key
		c.exchange, // source exchange
		false,      // no wait
		nil,        // args
	)
	if err != nil {
		return nil, errors.Wrap(err, "faied to bind queue")
	}

	return c.channel.Consume(
		queueName, // queue name
		"",        // consumer tag
		false,     // auto ack
		false,     // exclusive
		false,     // no local
		false,     // no wait
		nil,       // args
	)
}
