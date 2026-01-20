package app

import (
	"encoding/json"
)

// toJsonArray converts a string slice to a JSON array string
// This is used by the roles.templ file to pass data to JavaScript
func toJsonArray(arr []string) string {
	if len(arr) == 0 {
		return "[]"
	}
	bytes, err := json.Marshal(arr)
	if err != nil {
		return "[]"
	}
	return string(bytes)
}
