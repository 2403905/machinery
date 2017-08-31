package brokers

import (
	"encoding/json"
	"fmt"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"sync"
	"time"
	"github.com/satori/go.uuid"
)

var (
	kafkaDelayedTasksKey = "delayed_tasks"
)

type KafkaBroker struct {
	Broker
	consumer      *cluster.Consumer
	producer      sarama.AsyncProducer
	kafkaTopics  []string
	kafkaBrokers []string
	groupId string
	offset int64
	stopReceiving chan struct{}
}

func NewKafkaBroker(kafkaTopics, kafkaBrokers []string, groupId string, offset int64) (*KafkaBroker, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	log.INFO.Printf("Connecting to kafka brokers %s topics %s", kafkaBrokers, kafkaTopics)
	consumer, err := cluster.NewConsumer(kafkaBrokers, groupId, kafkaTopics, config)

	if err != nil {
		return nil, err
	}

	saramaConfig := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewAsyncProducer(kafkaBrokers, saramaConfig)

	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		consumer:      consumer,
		stopReceiving: make(chan struct{}),
		producer:      producer,
		kafkaBrokers: kafkaBrokers,
		kafkaTopics: kafkaTopics,
		groupId: groupId,
		offset: offset,
	}, nil
}

func (broker KafkaBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	broker.startConsuming(consumerTag, taskProcessor)
	deliveries := make(chan []byte)

	go func() {
		for {
			select {
			case err := <-broker.consumer.Errors():
				log.ERROR.Printf("Consumer error: ", err)
			case <-broker.stopReceiving:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case err := <-broker.producer.Errors():
				log.ERROR.Printf("Producer error: ", err)
			case <-broker.stopReceiving:
				return

			}
		}
	}()

	go func() {
		for {
			select {
			case note := <-broker.consumer.Notifications():
				log.WARNING.Printf("Rebalanced: %+v\n", note)
			case <-broker.stopReceiving:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case msg := <-broker.consumer.Messages():
				log.INFO.Print(msg)
				deliveries <- msg.Value
			case broker.stopReceiving:
				return
			}
		}
	}()

	if err := broker.consume(deliveries, taskProcessor); err != nil {
		return broker.retry, err
	}

	return false, nil
}

func (broker *KafkaBroker) StopConsuming() {
	if err := broker.consumer.Close(); err != nil {
		log.ERROR.Print(err)
	}

	broker.Broker.stopConsuming()

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
		msg := <- consumer.Messages()
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

func (broker *KafkaBroker) consume(deliveries <-chan []byte, taskProcessor TaskProcessor) error {
	maxWorkers := broker.cnf.MaxWorkerInstances
	pool := make(chan struct{}, maxWorkers)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < maxWorkers; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if maxWorkers != 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			wg.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				defer wg.Done()

				if err := broker.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				if maxWorkers != 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-broker.Broker.stopChan:
			return nil
		}
	}

	return nil
}

// consumeOne processes a single message using TaskProcessor
func (broker *KafkaBroker) consumeOne(delivery []byte, taskProcessor TaskProcessor) error {
	log.INFO.Printf("Received new message: %s", delivery)

	signature := new(tasks.Signature)
	if err := json.Unmarshal(delivery, signature); err != nil {
		return err
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !broker.IsTaskRegistered(signature.Name) {
		message := &sarama.ProducerMessage{Topic: broker.cnf.DefaultQueue, Value: sarama.ByteEncoder(delivery)}
		broker.producer.Input() <- message

		return nil
	}

	return taskProcessor.Process(signature)
}
