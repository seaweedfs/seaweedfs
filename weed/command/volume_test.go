package command

import (
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/log"
)

func TestXYZ(t *testing.T) {
	log.V(3).Infoln("Last-Modified", time.Unix(int64(1373273596), 0).UTC().Format(http.TimeFormat))
}
