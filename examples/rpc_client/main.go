package main

import (
	"log"

	rmq "github.com/askft/food-rmq"
)

func main() {
	client, err := rmq.NewRpcClient("amqp://guest:guest@127.0.0.1:5672")
	if err != nil {
		panic(err)
	}
	b, err := client.Do("", "rpc_queue", []byte("Hey there"))
	if err != nil {
		panic(err)
	}
	log.Printf("Got message %s", string(b))
}
