package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func NewConsumerConnection(address string) (*ConsumerConnection, error) {
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, err
	}
	return &ConsumerConnection{conn}, nil
}

func MustNewConsumerConnection(address string) *ConsumerConnection {
	pc, err := NewConsumerConnection(address)
	if err != nil {
		panic(err)
	}
	return pc
}

type ConsumerConnection struct {
	conn *amqp.Connection
}

type Consumer struct {
	Channel *amqp.Channel
	C       <-chan amqp.Delivery
}

// MustCreateConsumerStream panics if it fails
func (cc *ConsumerConnection) MustCreateStream(
	exchangeKind, exchangeName, queue, key string,
) *Consumer {

	stream, err := cc.CreateStream(exchangeKind, exchangeName, queue, key)
	if err != nil {
		panic(errors.Wrap(err, "failed to create stream"))
	}
	return stream
}

func (cc *ConsumerConnection) CreateStream(
	exchangeKind, exchangeName, queue, key string,
) (*Consumer, error) {

	channel, err := cc.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel")
	}

	if err := channel.ExchangeDeclare(
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

	if _, err := channel.QueueDeclare(
		queue, // queue name
		false, // durable
		true,  // auto delete?
		false, // exclusive?
		false, // no wait?
		nil,   // args
	); err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}

	if err := channel.QueueBind(
		queue,        // queue name
		key,          // binding key
		exchangeName, // source exchange
		false,        // no wait?
		nil,          // args
	); err != nil {
		return nil, errors.Wrap(err, "failed to bind queue")
	}

	c, err := channel.Consume(
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
	return &Consumer{channel, c}, nil
}
