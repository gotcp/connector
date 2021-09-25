package connector

import (
	"errors"
	"time"
)

const (
	_FUN_NAME_INITIALIZE = "Initialize"
	_FUN_NAME_POLL       = "Poll"
	_FUN_NAME_PUT        = "Put"
	_FUN_NAME_ON_MESSAGE = "OnMessage"
	_FUN_NAME_ON_ERROR   = "OnError"
)

const (
	_EMPTY_STRING = ""
)

const (
	_DEFAULT_DEALY = time.Millisecond * 100
)

const (
	_ERROR_CALL_FUN_TEMPLATE = "function name [%s] invalid"
)

var (
	_ERROR_CHILD_OBJECT_NULL = errors.New("child object is null")
)
