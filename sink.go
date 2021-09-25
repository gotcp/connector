package connector

import (
	"time"
)

type Sink struct {
	child         interface{}
	kafkaConsumer *KafkaConsumer
}

func (self *Sink) onError(err error) {
	CallFunc(self.child, _FUN_NAME_ON_ERROR, err)
}

func (self *Sink) onMessage(message []byte, offset int64, topic string, partition int32) bool {
	var vs, err = CallFunc(self.child, _FUN_NAME_PUT, message)
	if err != nil {
		CallFunc(self.child, _FUN_NAME_ON_ERROR, err)
		return false
	}

	var v interface{}
	if v = vs[0].Interface(); v != nil {
		if err = v.(error); err != nil {
			CallFunc(self.child, _FUN_NAME_ON_ERROR, err)
			return false
		}
	}

	return true
}

func (self *Sink) Start(child interface{}) {
	if child == nil {
		panic(_ERROR_CHILD_OBJECT_NULL)
	}
	self.child = child

	self.Initialize()
	CallFunc(self.child, _FUN_NAME_INITIALIZE)

	self.kafkaConsumer.Start()
}

func (self *Sink) Initialize() {
	self.kafkaConsumer = NewKafkaConsumer()
	self.kafkaConsumer.OnMessage = self.onMessage
	self.kafkaConsumer.OnError = self.onError
}

func (self *Sink) SetBrokers(brokers []string) {
	self.kafkaConsumer.SetBrokers(brokers)
}

func (self *Sink) SetGroup(group string) {
	self.kafkaConsumer.SetGroup(group)
}

func (self *Sink) SetTopics(topics []string) {
	self.kafkaConsumer.SetTopics(topics)
}

func (self *Sink) SetCommitInterval(t time.Duration) {
	self.kafkaConsumer.SetCommitInterval(t)
}

func (self *Sink) SetOffsetType(offsetType OffsetType) {
	self.kafkaConsumer.SetOffsetType(offsetType)
}
