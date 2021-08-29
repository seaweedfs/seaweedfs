package util

import (
	"fmt"
)

var (
	VERSION = fmt.Sprintf("%s %.02f", sizeLimit, 2.65)
	COMMIT  = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
