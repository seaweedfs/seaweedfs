package command

import (
	"strings"
)

func escapeKey(key string) string {
	if strings.Contains(key, ":") {
		return strings.ReplaceAll(key, ":", "")
	}
	return key
}
