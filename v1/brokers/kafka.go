package brokers

import (
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/bsm/sarama-cluster"
	"sync"
	"encoding/json"
)

type KafkaBroker struct {
	Broker
	consumer      *cluster.Consumer
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

	return &KafkaBroker{
		consumer:      consumer,
		stopReceiving: make(chan struct{}),
	}, nil
}

func (broker KafkaBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	broker.startConsuming(consumerTag, taskProcessor)
	deliveries := make(chan []byte)

	go func() {
		for {
			select {
			case err := <-broker.consumer.Errors():
				log.ERROR.Print(err)
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

func (broker *KafkaBroker) Publish(task *tasks.Signature) error {
	return nil
}

func (broker *KafkaBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, nil
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
		// TODO(stgleb): Add delivery back to queue
		return nil
	}

	return taskProcessor.Process(signature)
}