package rmq

/*
	https://www.rabbitmq.com/tutorials/tutorial-six-go.html
*/

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type RpcClient struct {
	Session Session
}

type RpcServer struct {
	Session Session
	Queue   amqp.Queue
}

func NewRpcClient(address string) (*RpcClient, error) {
	session, err := NewDefaultSession(address)
	if err != nil {
		return nil, err
	}

	return &RpcClient{
		session,
	}, nil
}

func NewRpcServer(address string) (*RpcServer, error) {
	session, err := NewDefaultSession(address)
	if err != nil {
		return nil, err
	}

	queue, err := session.Channel().QueueDeclare(
		"rpc_queue", false, false, false, false, nil,
	)
	if err != nil {
		return nil, err
	}

	return &RpcServer{
		session,
		queue,
	}, nil
}

func (c *RpcClient) Do(exchange, key string, body []byte) ([]byte, error) {

	queue, err := c.Session.Channel().QueueDeclare(
		"", false, false, true, false, nil,
	)
	if err != nil {
		return nil, err
	}

	corrId := "123456789"

	if err := c.Session.Channel().Publish(
		exchange, key, false, false, amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: corrId,
			Body:          body,
		},
	); err != nil {
		return nil, err
	}

	ds, err := c.Session.Channel().Consume(
		queue.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		return nil, err
	}

	for d := range ds {
		fmt.Println("got message", d)
		if corrId == d.CorrelationId {
			return d.Body, nil
		}
	}

	return nil, nil
}

func (s *RpcServer) Do(exchange string) {

	if err := s.Session.Channel().Qos(1, 0, false); err != nil {
		panic(err)
	}

	ds, err := s.Session.Channel().Consume(
		s.Queue.Name, "", false, false, false, false, nil,
	)
	if err != nil {
		panic(err)
	}

	loop := make(chan struct{})

	go func() {
		for d := range ds {
			if err := s.Session.Channel().Publish(
				exchange, d.ReplyTo, false, false, amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: d.CorrelationId,
					Body:          []byte(`{"great":"reponse"}`),
				},
			); err != nil {
				panic(err)
			}

			_ = d.Ack(false)
			log.Println("Sent message")
		}
	}()

	log.Println(" [*] Awaiting RPC requests")
	<-loop
}
