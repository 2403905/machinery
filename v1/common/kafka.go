package common

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// KafkaConnector ...
type KafkaConnector struct{}

func (kc *KafkaConnector) CreateConsumer(brokers []string, groupID string, topics []string, offset string) (*cluster.Consumer, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	var offs int64
	switch offset {
	case "oldest":
		offs = sarama.OffsetOldest
	case "newest":
		offs = sarama.OffsetNewest
	default:
		offs = sarama.OffsetNewest
	}

	config.Consumer.Offsets.Initial = offs
	consumer, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		return nil, fmt.Errorf("Kafka consumer creation error: %s", err)
	}

	return consumer, nil
}

func (kc *KafkaConnector) CreateProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Kafka producer creation error: %s", err)
	}

	return producer, nil
}

func (kc *KafkaConnector) CloseConsumer(c *cluster.Consumer) error {
	if c != nil {
		if err := c.Close(); err != nil {
			return fmt.Errorf("Close Consumer error: %s", err)
		}
	}
	return nil
}

func (kc *KafkaConnector) CloseProducer(p sarama.AsyncProducer) error {
	if p != nil {
		if err := p.Close(); err != nil {
			return fmt.Errorf("Close Consumer error: %s", err)
		}
	}
	return nil
}
