package security

import (
	"context"
	"sync/atomic"
)

// VolumeReadTokenProvider supplies the volume read credential for one file id.
// A client that must not hold jwt.signing.read.key -- a mount whose filer hands
// it exact-fid read capabilities (LookupDirectoryEntries read_auth) -- installs
// one; while installed it replaces local signing wherever a chunk is read
// directly from a volume server.
type VolumeReadTokenProvider func(ctx context.Context, fileId string) string

var volumeReadTokenProvider atomic.Pointer[VolumeReadTokenProvider]

// SetVolumeReadTokenProvider installs p process-wide; nil removes it.
func SetVolumeReadTokenProvider(p VolumeReadTokenProvider) {
	if p == nil {
		volumeReadTokenProvider.Store(nil)
		return
	}
	volumeReadTokenProvider.Store(&p)
}

// VolumeReadToken returns the installed provider's token for fileId, and
// whether a provider is installed. An installed provider that has no token
// returns "", so the read fails closed instead of falling back to a key.
func VolumeReadToken(ctx context.Context, fileId string) (string, bool) {
	p := volumeReadTokenProvider.Load()
	if p == nil {
		return "", false
	}
	return (*p)(ctx, fileId), true
}
