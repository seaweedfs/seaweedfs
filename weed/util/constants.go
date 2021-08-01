package util

import (
	"fmt"
)

var (
	VERSION = fmt.Sprintf("%s %.02f", sizeLimit, 2.61)
	COMMIT  = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
