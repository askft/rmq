package rmq

import (
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func NewPublisher(address string) (*Publisher, error) {
	session, err := connect(address)
	if err != nil {
		return nil, errors.Wrap(err, "could not create publisher")
	}
	return &Publisher{
		session,
	}, nil
}

type Publisher struct {
	*Session
}

func (p *Publisher) Send(exchange, key string, data []byte) error {
	err := p.channel.ExchangeDeclare(
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

	err = p.channel.Publish(
		exchange, // exchange
		key,      // binding key
		false,    // mandatory
		false,    // immediate
		msg,      // message
	)
	return errors.Wrap(err, "failed to publish")
}
