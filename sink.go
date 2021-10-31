package connector

import (
	"time"
)

type Sink struct {
	child         interface{}
	kafkaConsumer *KafkaConsumer
}

func (me *Sink) onError(err error) {
	CallFunc(me.child, _FUN_NAME_ON_ERROR, err)
}

func (me *Sink) onMessage(message []byte, offset int64, topic string, partition int32) bool {
	var vs, err = CallFunc(me.child, _FUN_NAME_PUT, message)
	if err != nil {
		CallFunc(me.child, _FUN_NAME_ON_ERROR, err)
		return false
	}

	var v interface{}
	if v = vs[0].Interface(); v != nil {
		if err = v.(error); err != nil {
			CallFunc(me.child, _FUN_NAME_ON_ERROR, err)
			return false
		}
	}

	return true
}

func (me *Sink) Start(child interface{}) {
	if child == nil {
		panic(_ERROR_CHILD_OBJECT_NULL)
	}
	me.child = child

	me.Initialize()
	CallFunc(me.child, _FUN_NAME_INITIALIZE)

	me.kafkaConsumer.Start()
}

func (me *Sink) Initialize() {
	me.kafkaConsumer = NewKafkaConsumer()
	me.kafkaConsumer.OnMessage = me.onMessage
	me.kafkaConsumer.OnError = me.onError
}

func (me *Sink) SetBrokers(brokers []string) {
	me.kafkaConsumer.SetBrokers(brokers)
}

func (me *Sink) SetGroup(group string) {
	me.kafkaConsumer.SetGroup(group)
}

func (me *Sink) SetTopics(topics []string) {
	me.kafkaConsumer.SetTopics(topics)
}

func (me *Sink) SetCommitInterval(t time.Duration) {
	me.kafkaConsumer.SetCommitInterval(t)
}

func (me *Sink) SetOffsetType(offsetType OffsetType) {
	me.kafkaConsumer.SetOffsetType(offsetType)
}
