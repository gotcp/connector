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

func (me *KafkaConsumer) Start() {
	var err error

	var config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = me.commitInterval
	config.Consumer.Offsets.Initial = me.offsetType

	var consumer *cluster.Consumer
	consumer, err = cluster.NewConsumer(
		me.brokers,
		me.group,
		me.topics,
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
			me.OnError(err)
		}
	}()

	var message *sarama.ConsumerMessage

	for {
		select {
		case message = <-consumer.Messages():
			if me.OnMessage(message.Value, message.Offset, message.Topic, message.Partition) {
				consumer.MarkOffset(message, _EMPTY_STRING)
			}
		case <-signals:
			os.Exit(0)
		}
	}
}

func (me *KafkaConsumer) SetBrokers(brokers []string) {
	me.brokers = brokers
}

func (me *KafkaConsumer) SetGroup(group string) {
	me.group = group
}

func (me *KafkaConsumer) SetTopics(topics []string) {
	me.topics = topics
}

func (me *KafkaConsumer) SetCommitInterval(t time.Duration) {
	me.commitInterval = t
}

func (me *KafkaConsumer) SetOffsetType(offsetType OffsetType) {
	if offsetType == OffsetOldest {
		me.offsetType = sarama.OffsetOldest
	} else {
		me.offsetType = sarama.OffsetNewest
	}
}
