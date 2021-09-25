package main

import (
	"fmt"

	"connector"
)

type MongoSource struct {
	connector.SourceSync
}

var (
	brokers = []string{"192.168.2.161:9092"}
	topic   = "test1"
)

func (self *MongoSource) Initialize() {
	fmt.Println("Initialize")
	self.SetBrokers(brokers)
	self.SetTopic(topic)
}

func (self *MongoSource) Poll() ([][]byte, error) {
	fmt.Println("Poll")
	return nil, nil
}

func (self *MongoSource) OnError(err error) {
	fmt.Println("#OnError ", err)
}

func main() {
	var c connector.ISource
	c = &MongoSource{}
	c.Start(c)
}
