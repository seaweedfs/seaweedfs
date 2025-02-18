package util

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
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

func ParseBool(s string, defaultValue bool) bool {
	value, err := strconv.ParseBool(s)
	if err != nil {
		return defaultValue
	}
	return value
}

func BoolToString(b bool) string {
	return strconv.FormatBool(b)
}

func ParseFilerUrl(entryPath string) (filerServer string, filerPort int64, path string, err error) {
	if !strings.HasPrefix(entryPath, "http://") && !strings.HasPrefix(entryPath, "https://") {
		entryPath = "http://" + entryPath
	}

	var u *url.URL
	u, err = url.Parse(entryPath)
	if err != nil {
		return
	}
	filerServer = u.Hostname()
	portString := u.Port()
	if portString != "" {
		filerPort, err = strconv.ParseInt(portString, 10, 32)
	}
	path = u.Path
	return
}

func ParseHostPort(hostPort string) (filerServer string, filerPort int64, err error) {
	parts := strings.Split(hostPort, ":")
	if len(parts) != 2 {
		err = fmt.Errorf("failed to parse %s\n", hostPort)
		return
	}

	filerPort, err = strconv.ParseInt(parts[1], 10, 64)
	if err == nil {
		filerServer = parts[0]
	}

	return
}

func CanonicalizeETag(etag string) string {
	canonicalETag := strings.TrimPrefix(etag, "\"")
	return strings.TrimSuffix(canonicalETag, "\"")
}
