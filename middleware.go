package rmq

import (
	"log"

	"github.com/streadway/amqp"
)

type Middleware func(MessageHandler) MessageHandler

func Recovery(next MessageHandler) MessageHandler {
	return func(d amqp.Delivery) error {
		defer func() {
			if rvr := recover(); rvr != nil {
				log.Println(rvr)
				// http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		_ = next(d)
		return nil
	}
}

// TODO
func Logging(next MessageHandler) MessageHandler {
	return func(d amqp.Delivery) error {
		log.Println("Received message with ID", d.MessageId)
		_ = next(d)
		return nil
	}
}

func (h MessageHandler) With(mw Middleware) MessageHandler {
	return mw(h)
}
