package rmq

import (
	"log"

	"github.com/streadway/amqp"
)

func AckOrLogPanic(d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		log.Panic(err)
	}
}

func RejectOrLogPanic(d amqp.Delivery) {
	if err := d.Reject(false); err != nil {
		log.Panic(err)
	}
}
