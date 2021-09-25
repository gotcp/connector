package connector

import (
	"reflect"
	"time"
)

type SourceSync struct {
	child         interface{}
	id            int
	delay         time.Duration
	kafkaProducer *SyncProducer
}

func (self *SourceSync) Start(child interface{}) {
	if child == nil {
		panic(_ERROR_CHILD_OBJECT_NULL)
	}
	self.child = child

	self.Initialize()
	CallFunc(self.child, _FUN_NAME_INITIALIZE)

	self.process()
}

func (self *SourceSync) Initialize() {
	self.delay = _DEFAULT_DEALY
	self.kafkaProducer = NewSyncProducer()
}

func (self *SourceSync) process() {
	var err error
	var vals []reflect.Value

	var v interface{}
	var list [][]byte
	var i, count int

	for {
		if vals, err = CallFunc(self.child, _FUN_NAME_POLL); err != nil {
			CallFunc(self.child, _FUN_NAME_ON_ERROR)
			time.Sleep(self.delay)
			continue
		}

		if v = vals[1].Interface(); v != nil {
			if err = v.(error); err != nil {
				CallFunc(self.child, _FUN_NAME_ON_ERROR, err)
				time.Sleep(self.delay)
				continue
			}
		}

		if v = vals[0].Interface(); v != nil {
			list = vals[0].Interface().([][]byte)
			count = len(list)
			for i = 0; i < count; i++ {
				err = self.kafkaProducer.SendBytes(self.kafkaProducer.topic, list[i])
				if err != nil {
					CallFunc(self.child, _FUN_NAME_ON_ERROR, err)
				}
			}
			if count == 0 {
				time.Sleep(self.delay)
			}
		} else {
			time.Sleep(self.delay)
		}
	}
}

func (self *SourceSync) SetBrokers(brokers []string) {
	self.kafkaProducer.SetBrokers(brokers)
}

func (self *SourceSync) SetTopic(topic string) {
	self.kafkaProducer.SetTopic(topic)
}

func (self *SourceSync) SetDelay(t time.Duration) {
	self.delay = t
}
