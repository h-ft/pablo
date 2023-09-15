package main

import (
	"fmt"
)

func main() {
	topic := "test_topics" // TODO: Read topic from config file

	c, err := initKafkaConsumer(topic)
	if err != nil {
		fmt.Errorf("Failed to initialize kafka producer: %s\n", err)
		return
	}

	batch := c.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	defer func() {

		if err := batch.Close(); err != nil {
			fmt.Errorf("failed to close batch: %v", err)
		}
		if err := c.Close(); err != nil {
			fmt.Errorf("failed to close connection: %v", err)
		}
	}()

	b := make([]byte, 10e3) // 10KB max per message
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}

}
