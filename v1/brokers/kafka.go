package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/vidmed/machinery/v1/common"
	"github.com/vidmed/machinery/v1/config"
	"github.com/vidmed/machinery/v1/log"
	"github.com/vidmed/machinery/v1/tasks"
	"sync"
)

var (
	kafkaDelayedTasksKey = "delayed_tasks"
)

type KafkaBroker struct {
	Broker
	common.KafkaConnector
	servers  []string
	consumer *cluster.Consumer
	producer sarama.AsyncProducer
}

func NewKafkaBroker(cnf *config.Config, servers []string) Interface {
	return &KafkaBroker{
		Broker:         New(cnf),
		KafkaConnector: common.KafkaConnector{},
		servers:        servers,
		consumer:       nil,
		producer:       nil,
	}
}

// StartConsuming enters a loop and waits for incoming messages
func (b *KafkaBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)

	var err error
	b.consumer, err = b.CreateConsumer(
		b.servers,
		consumerTag,
		b.cnf.Kafka.TopicList,
		b.cnf.Kafka.Offset,
	)
	if err != nil {
		b.retryFunc(b.retryStopChan)
		return b.retry, err
	}
	defer b.CloseConsumer(b.consumer)

	go func() { // subscribe on errors chanel
		for err := range b.consumer.Errors() {
			log.ERROR.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() { // subscribe on notifications chanel
		for note := range b.consumer.Notifications() {
			log.WARNING.Printf("Rebalanced: %+v\n", note)
		}
	}()

	log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

	if err := b.consume(b.consumer.Messages(), concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *KafkaBroker) StopConsuming() {
	b.stopConsuming()
}

// Publish places a new message on the default queue
func (b *KafkaBroker) Publish(signature *tasks.Signature) error {
	b.AdjustRoutingKey(signature)

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	//if signature.ETA != nil {
	//	now := time.Now().UTC()
	//
	//	if signature.ETA.After(now) {
	//		delayMs := int64(signature.ETA.Sub(now) / time.Millisecond)
	//
	//		return b.delay(signature, delayMs)
	//	}
	//}

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	b.producer, err = b.CreateProducer(b.servers)
	if err != nil {
		return err
	}
	defer b.CloseProducer(b.producer)
	// listen errors
	go func() {
		for err := range b.producer.Errors() {
			log.ERROR.Println(err.Error())
		}
	}()

	// Send task by signature routing key in order.
	msg := &sarama.ProducerMessage{Topic: signature.RoutingKey, Value: sarama.ByteEncoder(message)}
	b.producer.Input() <- msg

	return nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *KafkaBroker) consume(deliveries <-chan *sarama.ConsumerMessage, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with "concurrency" number workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)
	// Use wait group to make sure task processing completes on interrupt signal
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency != 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			wg.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				defer wg.Done()

				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.stopChan:
			return nil
		}
	}

	return nil
}

// consumeOne processes a single message using TaskProcessor
func (b *KafkaBroker) consumeOne(d *sarama.ConsumerMessage, taskProcessor TaskProcessor) error {
	defer func() {
		b.consumer.MarkOffset(d, "")
		err := b.consumer.CommitOffsets() // manually commits marked offsets after handling message
		if err != nil {
			log.ERROR.Printf("ERROR while committing offset into kafka - %s", err)
		}
	}()

	if len(d.Value) == 0 {
		return errors.New("Received an empty message")
	}

	log.INFO.Printf("Received new message: %s", d.Value)

	// Unmarshal message body into signature struct
	signature := new(tasks.Signature)
	if err := json.Unmarshal(d.Value, signature); err != nil {
		return err
	}

	// If the task is not registered, we DO NOT requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		log.ERROR.Printf("Task %q is not registered. Requeue", signature.Name)
		return nil
	}

	return taskProcessor.Process(signature)
}
