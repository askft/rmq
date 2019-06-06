package rmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func NewProducerConnection(address string) (*ProducerConnection, error) {
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, err
	}
	return &ProducerConnection{conn}, nil
}

func MustNewProducerConnection(address string) *ProducerConnection {
	pc, err := NewProducerConnection(address)
	if err != nil {
		panic(err)
	}
	return pc
}

type ProducerConnection struct {
	conn *amqp.Connection
}

type Producer struct {
	Channel      *amqp.Channel
	exchangeName string
	key          string
}

func (p *Producer) Send(msg amqp.Publishing) error {
	return p.Channel.Publish(
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

// Create a one-off producer and send a message through it
func (pc *ProducerConnection) Send(
	exchangeKind string,
	exchangeName string,
	key string,
	msg amqp.Publishing,
) error {
	p, err := pc.CreateStream(exchangeKind, exchangeName, key)
	if err != nil {
		return err
	}
	return p.Send(msg)
}

func (pc *ProducerConnection) MustCreateStream(
	exchangeKind string,
	exchangeName string,
	key string,
) *Producer {

	stream, err := pc.CreateStream(exchangeKind, exchangeName, key)
	if err != nil {
		panic(errors.Wrap(err, "failed to create producer"))
	}
	return stream
}

func (pc *ProducerConnection) CreateStream(
	exchangeKind string,
	exchangeName string,
	key string,
) (*Producer, error) {

	channel, err := pc.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "could not create channel")
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

	return &Producer{
		channel, exchangeName, key,
	}, nil
}
