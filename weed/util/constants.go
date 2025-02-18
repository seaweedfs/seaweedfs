package util

import (
	"fmt"
)

const HttpStatusCancelled = 499

var (
	MAJOR_VERSION  = int32(3)
	MINOR_VERSION  = int32(85)
	VERSION_NUMBER = fmt.Sprintf("%d.%02d", MAJOR_VERSION, MINOR_VERSION)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
