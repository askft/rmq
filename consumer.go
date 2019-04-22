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

func (c *Consumer) Receive(key string) (<-chan amqp.Delivery, error) {
	err := c.channel.ExchangeDeclare(c.exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare exchange")
	}

	queueName := "stuff"
	q, err := c.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}

	return c.channel.Consume(q.Name, "", true, false, false, false, nil)
}
