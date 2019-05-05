package rmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

type MessageJSON struct {
	CorrelationID string
	ReplyTo       string
	Body          []byte
}

func (m MessageJSON) String() string {
	return fmt.Sprintf("(CorrelationID: %q, ReplyTo: %q, Body: %s",
		m.CorrelationID, m.ReplyTo, string(m.Body))
}

func (m MessageJSON) ToAMQP() amqp.Publishing {
	return amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "application/json",
		CorrelationId: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		Body:          m.Body,
	}
}
