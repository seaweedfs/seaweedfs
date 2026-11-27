//go:build !plan9

package security

import (
	"github.com/seaweedfs/seaweedfs/weed/security/certreload"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

func init() {
	grace.OnReload(certreload.ReloadAll)
}
