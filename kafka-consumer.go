package connector

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type OffsetType int64

const (
	OffsetNewest OffsetType = -1
	OffsetOldest OffsetType = -2
)

type OnMessageEvent func(message []byte, offset int64, topic string, partition int32) bool
type OnErrorEvent func(err error)

type KafkaConsumer struct {
	brokers        []string
	group          string
	topics         []string
	offsetType     int64
	commitInterval time.Duration
	OnMessage      OnMessageEvent
	OnError        OnErrorEvent
}

func NewKafkaConsumer() *KafkaConsumer {
	return &KafkaConsumer{
		offsetType:     sarama.OffsetOldest,
		commitInterval: time.Second,
		OnMessage:      nil,
		OnError:        nil,
	}
}

func (self *KafkaConsumer) Start() {
	var err error

	var config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = self.commitInterval
	config.Consumer.Offsets.Initial = self.offsetType

	var consumer *cluster.Consumer
	consumer, err = cluster.NewConsumer(
		self.brokers,
		self.group,
		self.topics,
		config,
	)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	var signals = make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case err = <-consumer.Errors():
			self.OnError(err)
		}
	}()

	var message *sarama.ConsumerMessage

	for {
		select {
		case message = <-consumer.Messages():
			if self.OnMessage(message.Value, message.Offset, message.Topic, message.Partition) {
				consumer.MarkOffset(message, _EMPTY_STRING)
			}
		case <-signals:
			os.Exit(0)
		}
	}
}

func (self *KafkaConsumer) SetBrokers(brokers []string) {
	self.brokers = brokers
}

func (self *KafkaConsumer) SetGroup(group string) {
	self.group = group
}

func (self *KafkaConsumer) SetTopics(topics []string) {
	self.topics = topics
}

func (self *KafkaConsumer) SetCommitInterval(t time.Duration) {
	self.commitInterval = t
}

func (self *KafkaConsumer) SetOffsetType(offsetType OffsetType) {
	if offsetType == OffsetOldest {
		self.offsetType = sarama.OffsetOldest
	} else {
		self.offsetType = sarama.OffsetNewest
	}
}
