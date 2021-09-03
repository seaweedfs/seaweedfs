package util

import (
	"fmt"
)

var (
	VERSION = fmt.Sprintf("%.02f (%s)", 2.65, sizeLimit)
	COMMIT  = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
