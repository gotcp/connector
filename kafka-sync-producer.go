package connector

import (
	"github.com/Shopify/sarama"
)

type SyncProducer struct {
	producer sarama.SyncProducer
	client   sarama.Client
	brokers  []string
	topic    string
}

func NewSyncProducer() (producer *SyncProducer) {
	producer = &SyncProducer{}
	return
}

func newSyncProducerClient(brokers []string) (client sarama.Client, err error) {
	var config *sarama.Config
	config = sarama.NewConfig()
	config.Producer.Return.Successes = true
	client, err = sarama.NewClient(brokers, config)
	return
}

func newSyncProducer(client sarama.Client) (producer sarama.SyncProducer, err error) {
	producer, err = sarama.NewSyncProducerFromClient(client)
	return
}

func (self *SyncProducer) SetBrokers(brokers []string) {
	self.brokers = brokers
}

func (self *SyncProducer) SetTopic(topic string) {
	self.topic = topic
}

func (self *SyncProducer) Initialize() (err error) {
	var client sarama.Client
	client, err = newSyncProducerClient(self.brokers)
	if err != nil {
		return
	}

	var producer sarama.SyncProducer
	producer, err = newSyncProducer(client)
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

func (self *SyncProducer) isAvailable() bool {
	if self.client.Closed() || len(self.client.Brokers()) == 0 {
		return false
	}
	return true
}

func (self *SyncProducer) renew() (err error) {
	self.Close()

	var client sarama.Client
	client, err = newSyncProducerClient(self.brokers)
	if err != nil {
		return
	}

	var producer sarama.SyncProducer
	producer, err = newSyncProducer(client)
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

func (self *SyncProducer) renewIfNeed() (err error) {
	if !self.isAvailable() {
		err = self.renew()
	}
	return
}

func (self *SyncProducer) SendString(topic string, text string) (err error) {
	if err = self.renewIfNeed(); err != nil {
		return
	}
	_, _, err = self.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(text)})
	return
}

func (self *SyncProducer) SendBytes(topic string, text []byte) (err error) {
	if err = self.renewIfNeed(); err != nil {
		return
	}
	_, _, err = self.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(text)})
	return
}

func (self *SyncProducer) Close() {
	if self.producer != nil {
		self.producer.Close()
		self.producer = nil
	}
	if self.client != nil {
		if self.client.Closed() == false {
			self.client.Close()
			self.client = nil
		}
	}
}
