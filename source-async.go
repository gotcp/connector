package connector

import (
	"reflect"
	"time"
)

type SourceAsync struct {
	child         interface{}
	id            int
	delay         time.Duration
	kafkaProducer *AsyncProducer
}

func (self *SourceAsync) Start(child interface{}) {
	if child == nil {
		panic(_ERROR_CHILD_OBJECT_NULL)
	}
	self.child = child

	self.Initialize()
	CallFunc(self.child, _FUN_NAME_INITIALIZE)

	self.process()
}

func (self *SourceAsync) Initialize() {
	self.delay = _DEFAULT_DEALY
	self.kafkaProducer = NewAsyncProducer()
}

func (self *SourceAsync) process() {
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

func (self *SourceAsync) SetBrokers(brokers []string) {
	self.kafkaProducer.SetBrokers(brokers)
}

func (self *SourceAsync) SetTopic(topic string) {
	self.kafkaProducer.SetTopic(topic)
}

func (self *SourceAsync) SetDelay(t time.Duration) {
	self.delay = t
}
