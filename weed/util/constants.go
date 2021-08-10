package util

import (
	"fmt"
)

var (
	VERSION = fmt.Sprintf("%s %.02f", sizeLimit, 2.62)
	COMMIT  = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
