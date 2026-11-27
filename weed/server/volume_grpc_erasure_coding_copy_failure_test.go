package weed_server

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEcShardCopyFailureReason(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"source lost the shard", fmt.Errorf("VolumeEcShardsCopy volume 7: %w", errors.New("failed to copy /d/7.ec01 file: receiving: rpc error: code = Unknown desc = CopyFile not found ec volume id 7")), stats.EcShardCopySourceNotFound},
		{"not found status", status.Error(codes.NotFound, "volume 7"), stats.EcShardCopySourceNotFound},
		{"empty index", fmt.Errorf("VolumeEcShardsCopy volume 7: %w", errors.New("VolumeEcShardsCopy volume 7: source .ecx is 0 bytes")), stats.EcShardCopyEmptyIndex},
		{"no target disk", status.Error(codes.ResourceExhausted, "no space left"), stats.EcShardCopyNoSpace},
		{"disk full", fmt.Errorf("write /d/7.ec01.copying: %w", syscall.ENOSPC), stats.EcShardCopyNoSpace},
		{"deadline", fmt.Errorf("VolumeEcShardsCopy volume 7: %w", context.DeadlineExceeded), stats.EcShardCopyTimeout},
		{"deadline status", status.Error(codes.DeadlineExceeded, "stream"), stats.EcShardCopyTimeout},
		{"rust source deadline", errors.New("rpc error: code = Internal desc = copy 7.ec01: deadline has elapsed"), stats.EcShardCopyTimeout},
		{"network timeout", errors.New("read tcp 10.0.0.1:1->10.0.0.2:2: i/o timeout"), stats.EcShardCopyTimeout},
		{"io error", fmt.Errorf("write /d/7.ec01.copying: %w", syscall.EIO), stats.EcShardCopyIoError},
		{"other", errors.New("copied shards are encode run 1 but /d mounts run 2"), stats.EcShardCopyOther},
	}
	for _, c := range cases {
		if got := ecShardCopyFailureReason(c.err); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
