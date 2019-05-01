package rmq

import (
	"log"

	"github.com/streadway/amqp"
)

type Middleware func(MessageHandler) MessageHandler

func (h MessageHandler) With(mw Middleware) MessageHandler {
	return mw(h)
}

func Recovery(next MessageHandler) MessageHandler {
	return func(d amqp.Delivery) {
		defer func() {
			if rvr := recover(); rvr != nil {
				log.Println(rvr)
			}
		}()
		next(d)
	}
}

func Logging(next MessageHandler) MessageHandler {
	return func(d amqp.Delivery) {
		log.Println("Received message with ID", d.MessageId)
		next(d)
	}
}
