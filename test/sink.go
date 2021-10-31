package main

import (
	"fmt"

	"github.com/gotcp/connector"
)

type MongoSink struct {
	connector.Sink
}

var (
	brokers = []string{"192.168.2.161:9092"}
	topics  = []string{"test1"}
	group   = "1"
)

func (me *MongoSink) Initialize() {
	fmt.Println("Initialize")
	me.SetBrokers(brokers)
	me.SetTopics(topics)
	me.SetGroup(group)
	// c.SetCommitInterval()
}

func (me *MongoSink) Put(data []byte) error {
	fmt.Println("Put - ", string(data))
	return nil
}

func (c *MongoSink) OnError(err error) {
	fmt.Println("OnError - ", err)
}

func main() {
	var c connector.ISink
	c = &MongoSink{}
	c.Start(c)
}
