package main

import (
	"net/http"
	"testing"
	"time"
)

func TestXYZ(t *testing.T) {
	glog.V(4).Infoln("Last-Modified", time.Unix(int64(1373273596), 0).UTC().Format(http.TimeFormat))
}
