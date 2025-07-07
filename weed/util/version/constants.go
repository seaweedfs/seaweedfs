package version

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	MAJOR_VERSION  = int32(3)
	MINOR_VERSION  = int32(93)
	VERSION_NUMBER = fmt.Sprintf("%d.%02d", MAJOR_VERSION, MINOR_VERSION)
	VERSION        = util.SizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
