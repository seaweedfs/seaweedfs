package util

import "reflect"

// IsNotEmpty returns true if the given value is not zero or empty.
func IsNotEmpty(given interface{}) bool {
	return !IsEmpty(given)
}

// IsEmpty returns true if the given value has the zero value for its type.
func IsEmpty(given interface{}) bool {
	g := reflect.ValueOf(given)
	if !g.IsValid() {
		return true
	}

	if g.Kind() == reflect.Ptr {
		g = g.Elem()
	}

	// Basically adapted from text/template.isTrue
	switch g.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.String:
		return g.Len() == 0
	case reflect.Bool:
		return !g.Bool()
	case reflect.Complex64, reflect.Complex128:
		return g.Complex() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return g.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return g.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return g.Float() == 0
	case reflect.Struct:
		return g.IsZero()
	default:
		return g.IsNil()
	}
}
