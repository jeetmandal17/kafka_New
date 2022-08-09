package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topic = "server-update"
	brokerAddress1 = "localhost:9092"
	brokerAddress2 = "localhost:9093"
	brokerAddress3 = "localhost:9094"
)

func main(){

	// Create context for writing into topic
	ctx := context.Background()

	// initialize a writer to write into the kafka topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress1,brokerAddress2,brokerAddress3},
		Topic: topic,
		Async: true,
	})

	// Write message in topic
	for {
		
		// Write messages into the topic after 2 sec
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte("Key"),
			Value: []byte("Value"),
		},
		)

		fmt.Println("Message Written !")

		if err != nil {
			log.Fatal("err in writing message into topic")
		}

		time.Sleep(2*time.Second)
	}
}