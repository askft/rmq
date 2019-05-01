package rmq

import (
	"log"
	"strings"
	"sync"

	"github.com/streadway/amqp"
)

type Router struct {
	mailbox     Mailbox
	middlewares []Middleware
	handlers    map[string]MessageHandler
	wg          sync.WaitGroup
}

type MessageHandler func(amqp.Delivery)

func NewRouter(mailbox Mailbox) *Router {
	return &Router{
		mailbox,
		make([]Middleware, 0),
		make(map[string]MessageHandler),
		sync.WaitGroup{},
	}
}

func (r *Router) Use(mw Middleware) {
	r.middlewares = append(r.middlewares, mw)
}

func (r *Router) Bind(exchange, queue, key string, h MessageHandler) {
	r.handlers[exchange+":"+queue+":"+key] = h
}

func (r *Router) Run() error {
	for pattern, handler := range r.handlers {
		s := strings.Split(pattern, ":")
		exchange, queue, key := s[0], s[1], s[2]
		ds, err := r.mailbox.Receive(exchange, queue, key)
		if err != nil {
			return err
		}
		r.wg.Add(1)
		handler = chain(r.middlewares, handler)
		go r.handle(ds, handler)
	}
	r.wg.Wait()
	return nil
}

func chain(middlewares []Middleware, handler MessageHandler) MessageHandler {
	if len(middlewares) == 0 {
		return handler
	}

	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}

	return handler
}

func (r *Router) handle(c <-chan amqp.Delivery, h MessageHandler) {
	for d := range c {
		h(d)
		if err := d.Ack(false); err != nil {
			log.Fatal(err)
		}
	}
}
