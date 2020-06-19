package util

import (
	"fmt"
)

var (
	VERSION = fmt.Sprintf("%s %d.%d", sizeLimit, 1, 82)
	COMMIT  = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
