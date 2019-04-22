package rmq

import (
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func NewPublisher(address, exchange string) (*Publisher, error) {
	session, err := connect(address)
	if err != nil {
		return nil, errors.Wrap(err, "could not create publisher")
	}
	return &Publisher{
		session,
		exchange,
	}, nil
}

type Publisher struct {
	*Session
	ex string
}

func (p *Publisher) Send(key string, data []byte) error {
	err := p.channel.ExchangeDeclare(p.ex, "topic", true, false, false, false, nil)
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

	err = p.channel.Publish(p.ex, key, false, false, msg)
	return errors.Wrap(err, "failed to publish")
}
