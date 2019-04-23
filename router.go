package rmq

import (
	"log"
	"strings"
	"sync"

	"github.com/streadway/amqp"
)

type Router struct {
	session  *Session
	handlers map[string]MessageHandler
	wg       sync.WaitGroup
}

type MessageHandler func(amqp.Delivery) error

func NewRouter(session *Session) *Router {
	return &Router{
		session,
		make(map[string]MessageHandler),
		sync.WaitGroup{},
	}
}

func (r *Router) Bind(pattern string, h MessageHandler) {
	r.handlers[pattern] = h
}

func (r *Router) Run() error {
	for pattern, handler := range r.handlers {
		s := strings.Split(pattern, ":")
		exchange, queue, key := s[0], s[1], s[2]
		ds, err := r.session.Receive(exchange, queue, key)
		if err != nil {
			return err
		}
		r.wg.Add(1)
		go r.handle(ds, handler)
	}
	r.wg.Wait()
	return nil
}

func (r *Router) handle(c <-chan amqp.Delivery, h MessageHandler) {
	for d := range c {
		if err := h(d); err != nil {
			log.Println(err)
			continue
		}
		if err := d.Ack(false); err != nil {
			log.Fatal(err)
		}
	}
}
