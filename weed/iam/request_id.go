package iam

import "reflect"

type requestIDSetter interface {
	SetRequestId(string)
}

// WithRequestID sets the request ID on a response of known type. It uses
// generics to take the address of the value, satisfying the pointer receiver
// on SetRequestId without reflection.
func WithRequestID[T any, PT interface {
	*T
	SetRequestId(string)
}](response T, requestID string) T {
	PT(&response).SetRequestId(requestID)
	return response
}

// SetResponseRequestID sets the request ID on a response stored in interface{}.
// This is used by callers that build responses via a type switch and cannot use
// the generic WithRequestID. It uses reflection to take the address of value
// types whose SetRequestId method has a pointer receiver.
func SetResponseRequestID(response interface{}, requestID string) interface{} {
	if response == nil || requestID == "" {
		return response
	}

	value := reflect.ValueOf(response)
	if !value.IsValid() {
		return response
	}
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return response
	}

	if setter, ok := response.(requestIDSetter); ok {
		setter.SetRequestId(requestID)
		return response
	}

	if value.Kind() == reflect.Ptr {
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
