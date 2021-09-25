package connector

import (
	"errors"
	"fmt"
	"reflect"
)

func CallFunc(object interface{}, funcName string, params ...interface{}) (vals []reflect.Value, err error) {
	var ref = reflect.ValueOf(object)
	var method = ref.MethodByName(funcName)
	if method.IsValid() {
		var i int
		var count = len(params)
		var ps = make([]reflect.Value, 0)
		for i = 0; i < count; i++ {
			ps = append(ps, reflect.ValueOf(params[i]))
		}
		vals = method.Call(ps)
	} else {
		err = errors.New(fmt.Sprintf(_ERROR_CALL_FUN_TEMPLATE, funcName))
	}
	return
}
