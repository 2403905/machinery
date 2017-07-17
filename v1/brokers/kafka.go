package brokers

import (
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/Shopify/sarama"
	"machinery/v1/tasks"
)

type KafkaBroker struct {
	Broker
	kafkaClient sarama.AsyncProducer
}

func NewKafkaBroker(kafkaTopic string, kafkaBrokers []string) (*KafkaBroker, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	log.INFO.Printf("Connecting to kafka brokers %s", kafkaBrokers)
	client, err := sarama.NewAsyncProducer(kafkaBrokers, config)

	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		kafkaClient: client,
	}, nil
}

func (kafka KafkaBroker) SetRegisteredTaskNames(names []string) {

}

func (kafka KafkaBroker) IsTaskRegistered(name string) bool {
	return false
}

func (kafka KafkaBroker) StartConsuming(consumerTag string, p TaskProcessor) (bool, error) {
	return false, nil
}

func (kafka KafkaBroker) StopConsuming() {

}

func (kafka KafkaBroker) Publish(task *tasks.Signature) error {
	return nil
}

func (kafka KafkaBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, nil
}
