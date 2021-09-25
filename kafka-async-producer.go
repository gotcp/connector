package connector

import (
	"github.com/Shopify/sarama"
)

type AsyncProducer struct {
	producer sarama.AsyncProducer
	client   sarama.Client
	brokers  []string
	topic    string
}

func NewAsyncProducer() (producer *AsyncProducer) {
	producer = &AsyncProducer{}
	return
}

func newAsyncProducerClient(brokers []string) (client sarama.Client, err error) {
	var config *sarama.Config
	config = sarama.NewConfig()
	config.Producer.Return.Successes = true
	client, err = sarama.NewClient(brokers, config)
	return
}

func newAsyncProducer(client sarama.Client) (producer sarama.AsyncProducer, err error) {
	producer, err = sarama.NewAsyncProducerFromClient(client)
	return
}

func (self *AsyncProducer) SetBrokers(brokers []string) {
	self.brokers = brokers
}

func (self *AsyncProducer) SetTopic(topic string) {
	self.topic = topic
}

func (self *AsyncProducer) Initialize() (err error) {
	var client sarama.Client
	client, err = newAsyncProducerClient(self.brokers)
	if err != nil {
		return
	}

	var producer sarama.AsyncProducer
	producer, err = newAsyncProducer(client)
	if err != nil {
		if !client.Closed() {
			client.Close()
		}
		return
	}

	self.client = client
	self.producer = producer

	return
}

func (self *AsyncProducer) isAvailable() bool {
	if self.client.Closed() || len(self.client.Brokers()) == 0 {
		return false
	}
	return true
}

func (self *AsyncProducer) renew() (err error) {
	self.Close()

	var client sarama.Client
	client, err = newAsyncProducerClient(self.brokers)
	if err != nil {
		return
	}

	var producer sarama.AsyncProducer
	producer, err = newAsyncProducer(client)
	if err != nil {
		if client.Closed() == false {
			client.Close()
		}
		return
	}

	self.client = client
	self.producer = producer

	return
}

func (self *AsyncProducer) renewIfNeed() (err error) {
	if !self.isAvailable() {
		err = self.renew()
	}
	return
}

func (self *AsyncProducer) SendString(topic string, text string) (err error) {
	if err = self.renewIfNeed(); err != nil {
		return
	}
	self.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(text)}
	select {
	case err = <-self.producer.Errors():
		return
	default:
		return
	}
}

func (self *AsyncProducer) SendBytes(topic string, text []byte) (err error) {
	if err = self.renewIfNeed(); err != nil {
		return
	}
	self.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(text)}
	select {
	case err = <-self.producer.Errors():
		return
	default:
		return
	}
}

func (self *AsyncProducer) Close() {
	if self.producer != nil {
		self.producer.AsyncClose()
		self.producer = nil
	}
	if self.client != nil {
		if self.client.Closed() == false {
			self.client.Close()
			self.client = nil
		}
	}
}
