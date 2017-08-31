package brokers

import (
	"encoding/json"
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
	kafkaBrokers []string
}

func NewKafkaBroker(cnf *config.Config, kafkaBrokers []string) Interface {
	return &KafkaBroker{
		Broker:         New(cnf),
		KafkaConnector: common.KafkaConnector{},
		kafkaBrokers:   kafkaBrokers,
	}
}

func (b *KafkaBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)
	consumer, err := b.CreateConsumer(
		b.kafkaBrokers,
		consumerTag,
		b.cnf.Kafka.TopicList,
		b.cnf.Kafka.Offset,
	)
	if err != nil {
		b.retryFunc(b.retryStopChan)
		return b.retry, err
	}
	defer b.CloseConsumer(consumer)

	if err := b.consume(consumer.Messages(), taskProcessor); err != nil {
		return b.retry, err
	}

	return b.retry, nil
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

func (broker *KafkaBroker) consume(deliveries <-chan  *sarama.ConsumerMessage, taskProcessor TaskProcessor) error {
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
