package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

const (
	groupID = "gID-1"
	topic = "server-update"
	brokerAddress1 = "localhost:9092"
	brokerAddress2 = "localhost:9093"
	brokerAddress3 = "localhost:9094"
)

func main(){

	// Initailize the context
	ctx := context.Background()
	// initialize a kafka reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress1,brokerAddress2,brokerAddress3},
		Topic: topic,
		GroupID: groupID,
	})

	// Read message from the kafka queue
	for {
		m, err := r.ReadMessage(ctx)

		if err != nil {
			log.Fatal("error in reading message from queue")
		}
		fmt.Println("Message Received !, Offset:  ", m.Offset, "Key: ", m.Key, "Value: ", m.Value)
	}
}