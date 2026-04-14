package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
)

func newInodeSequencer(filerHost pb.ServerAddress) sequence.Sequencer {
	seq, err := sequence.NewSnowflakeSequencer(string(filerHost), 0)
	if err == nil {
		return seq
	}

	glog.Warningf("falling back to in-memory inode sequencer for filer %s: %v", filerHost, err)
	fallback := sequence.NewMemorySequencer()
	fallback.SetMax(1)
	return fallback
}

func (f *Filer) ensureEntryInode(entry *Entry) {
	if entry == nil || entry.Attr.Inode != 0 {
		return
	}
	entry.Attr.Inode = f.nextInode()
}

func (f *Filer) nextInode() uint64 {
	for {
		inode := f.inodeSequencer.NextFileId(1)
		if inode > 1 {
			return inode
		}
	}
}
