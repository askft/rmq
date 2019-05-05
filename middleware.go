package rmq

import (
	// "log"

	log "github.com/sirupsen/logrus"

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
		log.WithFields(log.Fields{
			"ReplyTo": d.ReplyTo,
			"CorrID":  d.CorrelationId,
			"Key":     d.RoutingKey,
		}).Infof("Received message %s.", string(d.Body))
		next(d)
	}
}
