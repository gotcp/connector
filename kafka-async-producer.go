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

func (me *AsyncProducer) SetBrokers(brokers []string) {
	me.brokers = brokers
}

func (me *AsyncProducer) SetTopic(topic string) {
	me.topic = topic
}

func (me *AsyncProducer) Initialize() (err error) {
	var client sarama.Client
	client, err = newAsyncProducerClient(me.brokers)
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

	me.client = client
	me.producer = producer

	return
}

func (me *AsyncProducer) isAvailable() bool {
	if me.client.Closed() || len(me.client.Brokers()) == 0 {
		return false
	}
	return true
}

func (me *AsyncProducer) renew() (err error) {
	me.Close()

	var client sarama.Client
	client, err = newAsyncProducerClient(me.brokers)
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

	me.client = client
	me.producer = producer

	return
}

func (me *AsyncProducer) renewIfNeed() (err error) {
	if !me.isAvailable() {
		err = me.renew()
	}
	return
}

func (me *AsyncProducer) SendString(topic string, text string) (err error) {
	if err = me.renewIfNeed(); err != nil {
		return
	}
	me.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(text)}
	select {
	case err = <-me.producer.Errors():
		return
	default:
		return
	}
}

func (me *AsyncProducer) SendBytes(topic string, text []byte) (err error) {
	if err = me.renewIfNeed(); err != nil {
		return
	}
	me.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(text)}
	select {
	case err = <-me.producer.Errors():
		return
	default:
		return
	}
}

func (me *AsyncProducer) Close() {
	if me.producer != nil {
		me.producer.AsyncClose()
		me.producer = nil
	}
	if me.client != nil {
		if me.client.Closed() == false {
			me.client.Close()
			me.client = nil
		}
	}
}
