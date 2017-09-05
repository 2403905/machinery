package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/satori/go.uuid"
	"sync"
	"time"
)

var (
	kafkaDelayedTasksKey = "delayed_tasks"
)

type KafkaBroker struct {
	Broker
	common.KafkaConnector
	servers []string
}

func NewKafkaBroker(cnf *config.Config, servers []string) Interface {
	return &KafkaBroker{
		Broker:         New(cnf),
		KafkaConnector: common.KafkaConnector{},
		servers:        servers,
	}
}

var con *cluster.Consumer

func (b *KafkaBroker) getConsumer(){
	if con == nil {

	}
	return con
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

func (broker *KafkaBroker) Publish(signature *tasks.Signature) error {
	msg, err := json.Marshal(signature)

	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	broker.AdjustRoutingKey(signature)

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	// TODO(stgleb): Somehow prioritize tasks according to ETA.
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			message := &sarama.ProducerMessage{Topic: broker.cnf.DefaultQueue, Value: sarama.ByteEncoder(msg)}
			broker.producer.Input() <- message

			return err
		}
	}

	// Send task by signature routing key in order.
	message := &sarama.ProducerMessage{Topic: signature.RoutingKey, Value: sarama.ByteEncoder(msg)}
	broker.producer.Input() <- message

	return nil
}

func (broker *KafkaBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	if queue == "" {
		queue = broker.cnf.DefaultQueue
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	consumer, err := cluster.NewConsumer(broker.kafkaBrokers, uuid.NewV4().String(), broker.kafkaTopics, config)

	if err != nil {
		log.ERROR.Printf("Error while getting pending tasks: %s", err)
	}

	var messages [][]byte
	queueLen := len(consumer.Messages())
	for i := 0; i < queueLen; i++ {
		msg := <-consumer.Messages()
		messages = append(messages, msg.Value)
	}

	taskSignatures := make([]*tasks.Signature, len(messages))
	for i, result := range messages {
		sig := new(tasks.Signature)
		if err := json.Unmarshal(result, sig); err != nil {
			return nil, err
		}
		taskSignatures[i] = sig
	}

	return taskSignatures, nil
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

	// If the task is not registered, we ack it and requeue,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		return nil
	}

	return taskProcessor.Process(signature)
}
