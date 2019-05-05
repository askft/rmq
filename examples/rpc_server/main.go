package main

import (
	rmq "github.com/askft/food-rmq"
)

func main() {
	server, err := rmq.NewRpcServer("amqp://guest:guest@127.0.0.1:5672")
	if err != nil {
		panic(err)
	}
	server.Do("")
}
