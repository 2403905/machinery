package brokers

import (
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/bsm/sarama-cluster"
	"machinery/v1/tasks"
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

func (broker *KafkaBroker) SetRegisteredTaskNames(names []string) {

}

func (broker *KafkaBroker) IsTaskRegistered(name string) bool {
	return false
}

func (broker KafkaBroker) StartConsuming(consumerTag string, p TaskProcessor) (bool, error) {
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

	go func() { // subscribe on notifications chanel
		for {
			select {
			case note := <-broker.consumer.Notifications():
				log.WARNING.Printf("Rebalanced: %+v\n", note)
			case <-broker.stopReceiving:
				return
			}
		}
	}()

	go func() { // push messages to the buffer
		for {
			select {
			case msg := <-broker.consumer.Messages():
				log.INFO.Print(msg)
			case broker.stopReceiving:
				return
			}
		}
	}()

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
