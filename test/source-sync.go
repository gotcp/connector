package main

import (
	"fmt"

	"github.com/gotcp/connector"
)

type MongoSource struct {
	connector.SourceSync
}

var (
	brokers = []string{"192.168.2.161:9092"}
	topic   = "test1"
)

func (me *MongoSource) Initialize() {
	fmt.Println("Initialize")
	me.SetBrokers(brokers)
	me.SetTopic(topic)
}

func (me *MongoSource) Poll() ([][]byte, error) {
	fmt.Println("Poll")
	return nil, nil
}

func (me *MongoSource) OnError(err error) {
	fmt.Println("OnError ", err)
}

func main() {
	var c connector.ISource
	c = &MongoSource{}
	c.Start(c)
}
