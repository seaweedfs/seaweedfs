package mount

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"math"
	"time"
)

const blockSize = 512

type statsCache struct {
	filer_pb.StatisticsResponse
	lastChecked int64 // unix time in seconds
}

func (wfs *WFS) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) (code fuse.Status) {

	// glog.V(4).Infof("reading fs stats")

	if wfs.stats.lastChecked < time.Now().Unix()-20 {

		err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.StatisticsRequest{
				Collection:  wfs.option.Collection,
				Replication: wfs.option.Replication,
				Ttl:         fmt.Sprintf("%ds", wfs.option.TtlSec),
				DiskType:    string(wfs.option.DiskType),
			}

			glog.V(4).Infof("reading filer stats: %+v", request)
			resp, err := client.Statistics(context.Background(), request)
			if err != nil {
				glog.V(0).Infof("reading filer stats %v: %v", request, err)
				return err
			}
			glog.V(4).Infof("read filer stats: %+v", resp)

			wfs.stats.TotalSize = resp.TotalSize
			wfs.stats.UsedSize = resp.UsedSize
			wfs.stats.FileCount = resp.FileCount
			wfs.stats.lastChecked = time.Now().Unix()

			return nil
		})
		if err != nil {
			glog.V(0).Infof("filer Statistics: %v", err)
			return fuse.OK
		}
	}

	totalDiskSize := wfs.stats.TotalSize
	usedDiskSize := wfs.stats.UsedSize
	actualFileCount := wfs.stats.FileCount

	if wfs.option.Quota > 0 && totalDiskSize > uint64(wfs.option.Quota) {
		totalDiskSize = uint64(wfs.option.Quota)
		if usedDiskSize > totalDiskSize {
			totalDiskSize = usedDiskSize
		}
	}

	// 	http://man.he.net/man2/statfs
	/*
		   struct statfs {
			   __fsword_t f_type;    // Type of filesystem (see below)
			   __fsword_t f_bsize;   // Optimal transfer block size
			   fsblkcnt_t f_blocks;  // Total data blocks in filesystem
			   fsblkcnt_t f_bfree;   // Free blocks in filesystem
			   fsblkcnt_t f_bavail;  // Free blocks available to
										unprivileged user
			   fsfilcnt_t f_files;   // Total file nodes in filesystem
			   fsfilcnt_t f_ffree;   // Free file nodes in filesystem
			   fsid_t     f_fsid;    // Filesystem ID
			   __fsword_t f_namelen; // Maximum length of filenames
			   __fsword_t f_frsize;  // Fragment size (since Linux 2.6)
			   __fsword_t f_flags;   // Mount flags of filesystem
										(since Linux 2.6.36)
			   __fsword_t f_spare[xxx];
							   // Padding bytes reserved for future use
		   };
	*/

	// Compute the total number of available blocks
	out.Blocks = totalDiskSize / blockSize
	if out.Blocks <= 0 {
		out.Blocks = 1
	}
	// Compute the number of used blocks
	numBlocks := usedDiskSize / blockSize

	remainingBlocks := int64(out.Blocks) - int64(numBlocks)
	if remainingBlocks < 0 {
		remainingBlocks = 0
	}

	// Report the number of free and available blocks for the block size
	out.Bfree = uint64(remainingBlocks)
	out.Bavail = uint64(remainingBlocks)
	out.Bsize = uint32(blockSize)

	// Report the total number of possible files in the file system (and those free)
	out.Files = math.MaxInt64
	out.Ffree = math.MaxInt64 - actualFileCount

	// Report the maximum length of a name and the minimum fragment size
	out.NameLen = 1024
	out.Frsize = uint32(blockSize)

	return fuse.OK
}
