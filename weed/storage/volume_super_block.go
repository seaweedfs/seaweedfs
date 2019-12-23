package storage

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

func (v *Volume) maybeWriteSuperBlock() error {

	datSize, _, e := v.DataBackend.GetStat()
	if e != nil {
		glog.V(0).Infof("failed to stat datafile %s: %v", v.DataBackend.Name(), e)
		return e
	}
	if datSize == 0 {
		v.SuperBlock.Version = needle.CurrentVersion
		_, e = v.DataBackend.WriteAt(v.SuperBlock.Bytes(), 0)
		if e != nil && os.IsPermission(e) {
			//read-only, but zero length - recreate it!
			var dataFile *os.File
			if dataFile, e = os.Create(v.DataBackend.Name()); e == nil {
				v.DataBackend = backend.NewDiskFile(dataFile)
				if _, e = v.DataBackend.WriteAt(v.SuperBlock.Bytes(), 0); e == nil {
					v.noWriteOrDelete = false
					v.noWriteCanDelete = false
				}
			}
		}
	}
	return e
}

func (v *Volume) readSuperBlock() (err error) {
	v.SuperBlock, err = super_block.ReadSuperBlock(v.DataBackend)
	return err
}
