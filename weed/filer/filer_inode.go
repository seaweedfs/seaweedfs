package filer

import (
	"os"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
)

// newInodeSequencer constructs the inode sequencer used to assign object
// identity for filer entries. The Snowflake node id defaults to a masked hash
// of filerHost, which only has 1024 possible values; operators running a
// multi-filer cluster should set SEAWEEDFS_FILER_SNOWFLAKE_ID to an explicit
// per-filer value (1..1023) to avoid birthday-paradox collisions.
//
// Initialization failures are fatal: a process-local fallback allocator would
// re-use inode values across restarts and violate the stable object identity
// guarantee that NFS filehandles and the inode secondary index rely on.
func newInodeSequencer(filerHost pb.ServerAddress) sequence.Sequencer {
	snowflakeId := parseSnowflakeIdFromEnv()
	seq, err := sequence.NewSnowflakeSequencer(string(filerHost), snowflakeId)
	if err != nil {
		glog.Fatalf("initialize inode sequencer for filer %s (snowflakeId=%d): %v", filerHost, snowflakeId, err)
	}
	return seq
}

func parseSnowflakeIdFromEnv() int {
	raw := os.Getenv("SEAWEEDFS_FILER_SNOWFLAKE_ID")
	if raw == "" {
		return 0
	}
	id, err := strconv.Atoi(raw)
	if err != nil || id < 0 || id > 0x3ff {
		glog.Fatalf("SEAWEEDFS_FILER_SNOWFLAKE_ID must be an integer in [0,1023], got %q", raw)
	}
	return id
}

func (f *Filer) ensureEntryInode(entry *Entry) {
	if entry == nil || entry.Attr.Inode != 0 {
		return
	}
	entry.Attr.Inode = f.nextInode()
}

func (f *Filer) nextInode() uint64 {
	return f.inodeSequencer.NextFileId(1)
}
