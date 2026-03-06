package iam

import "reflect"

type requestIDSetter interface {
	SetRequestId(string)
}

// SetResponseRequestID populates the request ID on either pointer or value responses.
func SetResponseRequestID(response interface{}, requestID string) interface{} {
	if response == nil || requestID == "" {
		return response
	}

	if setter, ok := response.(requestIDSetter); ok {
		setter.SetRequestId(requestID)
		return response
	}

	value := reflect.ValueOf(response)
	if !value.IsValid() || value.Kind() == reflect.Ptr {
		return response
	}

	ptr := reflect.New(value.Type())
	ptr.Elem().Set(value)

	if setter, ok := ptr.Interface().(requestIDSetter); ok {
		setter.SetRequestId(requestID)
		return ptr.Interface()
	}

	return response
}
