package brokers

import "machinery/v1/tasks"

type KafkaBroker struct {
	Broker
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
