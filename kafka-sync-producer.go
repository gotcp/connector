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

func (me *SyncProducer) SetBrokers(brokers []string) {
	me.brokers = brokers
}

func (me *SyncProducer) SetTopic(topic string) {
	me.topic = topic
}

func (me *SyncProducer) Initialize() (err error) {
	var client sarama.Client
	client, err = newSyncProducerClient(me.brokers)
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

	me.client = client
	me.producer = producer

	return
}

func (me *SyncProducer) isAvailable() bool {
	if me.client.Closed() || len(me.client.Brokers()) == 0 {
		return false
	}
	return true
}

func (me *SyncProducer) renew() (err error) {
	me.Close()

	var client sarama.Client
	client, err = newSyncProducerClient(me.brokers)
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

	me.client = client
	me.producer = producer

	return
}

func (me *SyncProducer) renewIfNeed() (err error) {
	if !me.isAvailable() {
		err = me.renew()
	}
	return
}

func (me *SyncProducer) SendString(topic string, text string) (err error) {
	if err = me.renewIfNeed(); err != nil {
		return
	}
	_, _, err = me.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(text)})
	return
}

func (me *SyncProducer) SendBytes(topic string, text []byte) (err error) {
	if err = me.renewIfNeed(); err != nil {
		return
	}
	_, _, err = me.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(text)})
	return
}

func (me *SyncProducer) Close() {
	if me.producer != nil {
		me.producer.Close()
		me.producer = nil
	}
	if me.client != nil {
		if me.client.Closed() == false {
			me.client.Close()
			me.client = nil
		}
	}
}
