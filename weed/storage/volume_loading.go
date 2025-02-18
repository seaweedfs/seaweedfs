package storage

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func loadVolumeWithoutIndex(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapKind) (v *Volume, err error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = super_block.SuperBlock{}
	v.needleMapKind = needleMapKind
	err = v.load(false, false, needleMapKind, 0)
	return
}

func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, needleMapKind NeedleMapKind, preallocate int64) (err error) {
	alreadyHasSuperBlock := false

	hasLoadedVolume := false
	defer func() {
		if !hasLoadedVolume {
			if v.nm != nil {
				v.nm.Close()
				v.nm = nil
			}
			if v.DataBackend != nil {
				v.DataBackend.Close()
				v.DataBackend = nil
			}
		}
	}()

	hasVolumeInfoFile := v.maybeLoadVolumeInfo()

	if v.volumeInfo.ReadOnly && !v.HasRemoteFile() {
		// this covers the case where the volume is marked as read-only and has no remote file
		v.noWriteOrDelete = true
	}

	if v.HasRemoteFile() {
		v.noWriteCanDelete = true
		v.noWriteOrDelete = false
		glog.V(0).Infof("loading volume %d from remote %v", v.Id, v.volumeInfo)
		if err := v.LoadRemoteFile(); err != nil {
			return fmt.Errorf("load remote file %v: %v", v.volumeInfo, err)
		}
		alreadyHasSuperBlock = true
	} else if exists, canRead, canWrite, modifiedTime, fileSize := util.CheckFile(v.FileName(".dat")); exists {
		// open dat file
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s", v.FileName(".dat"))
		}
		var dataFile *os.File
		if canWrite {
			dataFile, err = os.OpenFile(v.FileName(".dat"), os.O_RDWR|os.O_CREATE, 0644)
		} else {
			glog.V(0).Infof("opening %s in READONLY mode", v.FileName(".dat"))
			dataFile, err = os.Open(v.FileName(".dat"))
			v.noWriteOrDelete = true
		}
		v.lastModifiedTsSeconds = uint64(modifiedTime.Unix())
		if fileSize >= super_block.SuperBlockSize {
			alreadyHasSuperBlock = true
		}
		v.DataBackend = backend.NewDiskFile(dataFile)
	} else {
		if createDatIfMissing {
			v.DataBackend, err = backend.CreateVolumeFile(v.FileName(".dat"), preallocate, v.MemoryMapMaxSizeMb)
		} else {
			return fmt.Errorf("volume data file %s does not exist", v.FileName(".dat"))
		}
	}

	if err != nil {
		if !os.IsPermission(err) {
			return fmt.Errorf("cannot load volume data %s: %v", v.FileName(".dat"), err)
		} else {
			return fmt.Errorf("load data file %s: %v", v.FileName(".dat"), err)
		}
	}

	if alreadyHasSuperBlock {
		err = v.readSuperBlock()
		if err == nil {
			v.volumeInfo.Version = uint32(v.SuperBlock.Version)
		}
		glog.V(0).Infof("readSuperBlock volume %d version %v", v.Id, v.SuperBlock.Version)
		if v.HasRemoteFile() {
			// maybe temporary network problem
			glog.Errorf("readSuperBlock remote volume %d: %v", v.Id, err)
			err = nil
		}
	} else {
		if !v.SuperBlock.Initialized() {
			return fmt.Errorf("volume %s not initialized", v.FileName(".dat"))
		}
		err = v.maybeWriteSuperBlock()
	}
	if err == nil && alsoLoadIndex {
		// adjust for existing volumes with .idx together with .dat files
		if v.dirIdx != v.dir {
			if util.FileExists(v.DataFileName() + ".idx") {
				v.dirIdx = v.dir
			}
		}
		// check volume idx files
		if err := v.checkIdxFile(); err != nil {
			glog.Fatalf("check volume idx file %s: %v", v.FileName(".idx"), err)
		}
		var indexFile *os.File
		if v.noWriteOrDelete {
			glog.V(0).Infoln("open to read file", v.FileName(".idx"))
			if indexFile, err = os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644); err != nil {
				return fmt.Errorf("cannot read Volume Index %s: %v", v.FileName(".idx"), err)
			}
		} else {
			glog.V(1).Infoln("open to write file", v.FileName(".idx"))
			if indexFile, err = os.OpenFile(v.FileName(".idx"), os.O_RDWR|os.O_CREATE, 0644); err != nil {
				return fmt.Errorf("cannot write Volume Index %s: %v", v.FileName(".idx"), err)
			}
		}
		// Do not need to check the data integrity for remote volumes,
		// since the remote storage tier may have larger capacity, the volume
		// data read will trigger the ReadAt() function to read from the remote
		// storage tier, and download to local storage, which may cause the
		// capactiy overloading.
		if !v.HasRemoteFile() {
			glog.V(0).Infof("checking volume data integrity for volume %d", v.Id)
			if v.lastAppendAtNs, err = CheckVolumeDataIntegrity(v, indexFile); err != nil {
				v.noWriteOrDelete = true
				glog.V(0).Infof("volumeDataIntegrityChecking failed %v", err)
			}
		}

		if v.noWriteOrDelete || v.noWriteCanDelete {
			if v.nm, err = NewSortedFileNeedleMap(v.IndexFileName(), indexFile); err != nil {
				glog.V(0).Infof("loading sorted db %s error: %v", v.FileName(".sdx"), err)
			}
		} else {
			switch needleMapKind {
			case NeedleMapInMemory:
				if v.tmpNm != nil {
					glog.V(0).Infof("updating memory compact index %s ", v.FileName(".idx"))
					err = v.tmpNm.UpdateNeedleMap(v, indexFile, nil, 0)
				} else {
					glog.V(0).Infoln("loading memory index", v.FileName(".idx"), "to memory")
					if v.nm, err = LoadCompactNeedleMap(indexFile); err != nil {
						glog.V(0).Infof("loading index %s to memory error: %v", v.FileName(".idx"), err)
					}
				}
			case NeedleMapLevelDb:
				opts := &opt.Options{
					BlockCacheCapacity:            2 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   1 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.tmpNm != nil {
					glog.V(0).Infoln("updating leveldb index", v.FileName(".ldb"))
					err = v.tmpNm.UpdateNeedleMap(v, indexFile, opts, v.ldbTimeout)
				} else {
					glog.V(0).Infoln("loading leveldb index", v.FileName(".ldb"))
					if v.nm, err = NewLevelDbNeedleMap(v.FileName(".ldb"), indexFile, opts, v.ldbTimeout); err != nil {
						glog.V(0).Infof("loading leveldb %s error: %v", v.FileName(".ldb"), err)
					}
				}
			case NeedleMapLevelDbMedium:
				opts := &opt.Options{
					BlockCacheCapacity:            4 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   2 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.tmpNm != nil {
					glog.V(0).Infoln("updating leveldb medium index", v.FileName(".ldb"))
					err = v.tmpNm.UpdateNeedleMap(v, indexFile, opts, v.ldbTimeout)
				} else {
					glog.V(0).Infoln("loading leveldb medium index", v.FileName(".ldb"))
					if v.nm, err = NewLevelDbNeedleMap(v.FileName(".ldb"), indexFile, opts, v.ldbTimeout); err != nil {
						glog.V(0).Infof("loading leveldb %s error: %v", v.FileName(".ldb"), err)
					}
				}
			case NeedleMapLevelDbLarge:
				opts := &opt.Options{
					BlockCacheCapacity:            8 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   4 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.tmpNm != nil {
					glog.V(0).Infoln("updating leveldb large index", v.FileName(".ldb"))
					err = v.tmpNm.UpdateNeedleMap(v, indexFile, opts, v.ldbTimeout)
				} else {
					glog.V(0).Infoln("loading leveldb large index", v.FileName(".ldb"))
					if v.nm, err = NewLevelDbNeedleMap(v.FileName(".ldb"), indexFile, opts, v.ldbTimeout); err != nil {
						glog.V(0).Infof("loading leveldb %s error: %v", v.FileName(".ldb"), err)
					}
				}
			}
		}
	}

	if !hasVolumeInfoFile {
		v.volumeInfo.Version = uint32(v.SuperBlock.Version)
		v.volumeInfo.BytesOffset = uint32(types.OffsetSize)
		if err := v.SaveVolumeInfo(); err != nil {
			glog.Warningf("volume %d failed to save file info: %v", v.Id, err)
		}
	}

	stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "volume").Inc()

	if err == nil {
		hasLoadedVolume = true
	}

	return err
}
