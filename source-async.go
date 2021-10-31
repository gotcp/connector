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

func (me *SourceAsync) Start(child interface{}) {
	if child == nil {
		panic(_ERROR_CHILD_OBJECT_NULL)
	}
	me.child = child

	me.Initialize()
	CallFunc(me.child, _FUN_NAME_INITIALIZE)

	me.process()
}

func (me *SourceAsync) Initialize() {
	me.delay = _DEFAULT_DEALY
	me.kafkaProducer = NewAsyncProducer()
}

func (me *SourceAsync) process() {
	var err error
	var vals []reflect.Value

	var v interface{}
	var list [][]byte
	var i, count int

	for {
		if vals, err = CallFunc(me.child, _FUN_NAME_POLL); err != nil {
			CallFunc(me.child, _FUN_NAME_ON_ERROR)
			time.Sleep(me.delay)
			continue
		}

		if v = vals[1].Interface(); v != nil {
			if err = v.(error); err != nil {
				CallFunc(me.child, _FUN_NAME_ON_ERROR, err)
				time.Sleep(me.delay)
				continue
			}
		}

		if v = vals[0].Interface(); v != nil {
			list = vals[0].Interface().([][]byte)
			count = len(list)
			for i = 0; i < count; i++ {
				err = me.kafkaProducer.SendBytes(me.kafkaProducer.topic, list[i])
				if err != nil {
					CallFunc(me.child, _FUN_NAME_ON_ERROR, err)
				}
			}
			if count == 0 {
				time.Sleep(me.delay)
			}
		} else {
			time.Sleep(me.delay)
		}
	}
}

func (me *SourceAsync) SetBrokers(brokers []string) {
	me.kafkaProducer.SetBrokers(brokers)
}

func (me *SourceAsync) SetTopic(topic string) {
	me.kafkaProducer.SetTopic(topic)
}

func (me *SourceAsync) SetDelay(t time.Duration) {
	me.delay = t
}
