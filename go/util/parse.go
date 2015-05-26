package util

import (
	"strconv"
)

func ParseInt(text string, defaultValue int) int {
	count, parseError := strconv.ParseInt(text, 10, 64)
	if parseError != nil {
		if len(text) > 0 {
			return 0
		}
		return defaultValue
	}
	return int(count)
}
func ParseUint64(text string, defaultValue uint64) uint64 {
	count, parseError := strconv.ParseUint(text, 10, 64)
	if parseError != nil {
		if len(text) > 0 {
			return 0
		}
		return defaultValue
	}
	return count
}
