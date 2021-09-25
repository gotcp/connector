package connector

import (
	"time"
)

type IConnector interface {
	Start(child interface{})
	Initialize()
	SetBrokers(brokers []string)
	OnError(err error)
}

type ISource interface {
	IConnector
	Poll() ([][]byte, error)
	SetDelay(t time.Duration)
}

type ISink interface {
	IConnector
	Put(data []byte) error
	SetGroup(group string)
	SetTopics(topics []string)
	SetOffsetType(offsetType OffsetType)
	SetCommitInterval(t time.Duration)
}
