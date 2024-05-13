package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// Order represents the structure of the incoming order data
type Order struct {
	OrderID      string `json:"orderId"`
	CustomerName string `json:"customerName"`
	OrderDate    string `json:"orderDate"`
	Items        []Item `json:"items"`
}

// Item represents individual items in an order
type Item struct {
	ItemID   string `json:"itemId"`
	Quantity int    `json:"quantity"`
}

// ProcessedOrder is the enriched order structure that includes the processing status
type ProcessedOrder struct {
	Order
	Status string `json:"status"`
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0

	// Enable producer and consumer configurations
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true

	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create cluster admin: %v", err)
	}
	defer admin.Close()

	// Ensure topics exist
	topicList := []string{"raw-orders", "processed-orders"}
	for _, topic := range topicList {
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		err := admin.CreateTopic(topic, topicDetail, false)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				log.Fatalf("Failed to create topic %s: %v", topic, err)
			}
		}
	}

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "order-processor-group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// Consume from the raw-orders topic
			if err := consumerGroup.Consume(ctx, []string{"raw-orders"}, &Consumer{}); err != nil {
				log.Printf("Error consuming from group: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the consumer
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	cancel()
	wg.Wait()
	log.Println("Shutting down consumer group")
}

// Consumer implements the sarama.ConsumerGroupHandler interface
type Consumer struct{}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var order Order
		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("Error unmarshalling order: %v", err)
			continue
		}

		processedOrder := ProcessedOrder{
			Order:  order,
			Status: "Processed",
		}

		processedData, err := json.Marshal(processedOrder)
		if err != nil {
			log.Printf("Error marshalling processed order: %v", err)
			continue
		}

		producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
		if err != nil {
			log.Printf("Failed to start Sarama producer: %v", err)
			continue
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "processed-orders",
			Value: sarama.ByteEncoder(processedData),
		})
		if err != nil {
			log.Printf("Failed to send processed message: %v", err)
			continue
		}
		producer.Close()

		fmt.Printf("Processed and sent order: %s\n", processedOrder.OrderID)
		session.MarkMessage(message, "")
	}

	return nil
}
