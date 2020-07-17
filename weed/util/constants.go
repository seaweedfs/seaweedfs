package util

import (
	"fmt"
)

var (
	VERSION = fmt.Sprintf("%s %d.%d", sizeLimit, 1, 85)
	COMMIT  = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
