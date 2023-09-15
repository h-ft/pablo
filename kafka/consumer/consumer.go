package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func initKafkaConsumer(topic string) (*kafka.Conn, error) {
	// to consume messages
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	return conn, nil
}
