package chunk_cache

import (
	"fmt"
	"os"
	"time"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// This implements an on disk cache
// The entries are an FIFO with a size limit

type ChunkCacheVolume struct {
	DataBackend backend.BackendStorageFile
	nm          storage.NeedleMapper
	fileName    string
	smallBuffer []byte
	sizeLimit   int64
	lastModTime time.Time
	fileSize    int64
}

func LoadOrCreateChunkCacheVolume(fileName string, preallocate int64) (*ChunkCacheVolume, error) {

	v := &ChunkCacheVolume{
		smallBuffer: make([]byte, types.NeedlePaddingSize),
		fileName:    fileName,
		sizeLimit:   preallocate,
	}

	var err error

	if exists, canRead, canWrite, modTime, fileSize := util.CheckFile(v.fileName + ".dat"); exists {
		if !canRead {
			return nil, fmt.Errorf("cannot read cache file %s.dat", v.fileName)
		}
		if !canWrite {
			return nil, fmt.Errorf("cannot write cache file %s.dat", v.fileName)
		}
		if dataFile, err := os.OpenFile(v.fileName+".dat", os.O_RDWR|os.O_CREATE, 0644); err != nil {
			return nil, fmt.Errorf("cannot create cache file %s.dat: %v", v.fileName, err)
		} else {
			v.DataBackend = backend.NewDiskFile(dataFile)
			v.lastModTime = modTime
			v.fileSize = fileSize
		}
	} else {
		if v.DataBackend, err = backend.CreateVolumeFile(v.fileName+".dat", preallocate, 0); err != nil {
			return nil, fmt.Errorf("cannot create cache file %s.dat: %v", v.fileName, err)
		}
		v.lastModTime = time.Now()
	}

	var indexFile *os.File
	if indexFile, err = os.OpenFile(v.fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot write cache index %s.idx: %v", v.fileName, err)
	}

	glog.V(1).Infoln("loading leveldb", v.fileName+".ldb")
	opts := &opt.Options{
		BlockCacheCapacity:            2 * 1024 * 1024, // default value is 8MiB
		WriteBuffer:                   1 * 1024 * 1024, // default value is 4MiB
		CompactionTableSizeMultiplier: 10,              // default value is 1
	}
	if v.nm, err = storage.NewLevelDbNeedleMap(v.fileName+".ldb", indexFile, opts, 0); err != nil {
		return nil, fmt.Errorf("loading leveldb %s error: %v", v.fileName+".ldb", err)
	}

	return v, nil

}

func (v *ChunkCacheVolume) Shutdown() {
	if v.DataBackend != nil {
		v.DataBackend.Close()
		v.DataBackend = nil
	}
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
}

func (v *ChunkCacheVolume) doReset() {
	v.Shutdown()
	os.Truncate(v.fileName+".dat", 0)
	os.Truncate(v.fileName+".idx", 0)
	glog.V(4).Infof("cache removeAll %s ...", v.fileName+".ldb")
	os.RemoveAll(v.fileName + ".ldb")
	glog.V(4).Infof("cache removed %s", v.fileName+".ldb")
}

func (v *ChunkCacheVolume) Reset() (*ChunkCacheVolume, error) {
	v.doReset()
	return LoadOrCreateChunkCacheVolume(v.fileName, v.sizeLimit)
}

func (v *ChunkCacheVolume) GetNeedle(key types.NeedleId) ([]byte, error) {

	nv, ok := v.nm.Get(key)
	if !ok {
		return nil, storage.ErrorNotFound
	}
	data := make([]byte, nv.Size)
	if readSize, readErr := v.DataBackend.ReadAt(data, nv.Offset.ToActualOffset()); readErr != nil {
		if readSize != int(nv.Size) {
			return nil, fmt.Errorf("read %s.dat [%d,%d): %v",
				v.fileName, nv.Offset.ToActualOffset(), nv.Offset.ToActualOffset()+int64(nv.Size), readErr)
		}
	} else {
		if readSize != int(nv.Size) {
			return nil, fmt.Errorf("read %d, expected %d", readSize, nv.Size)
		}
	}

	return data, nil
}

func (v *ChunkCacheVolume) getNeedleSlice(key types.NeedleId, offset, length uint64) ([]byte, error) {
	nv, ok := v.nm.Get(key)
	if !ok {
		return nil, storage.ErrorNotFound
	}
	wanted := min(int(length), int(nv.Size)-int(offset))
	if wanted < 0 {
		// should never happen, but better than panicking
		return nil, ErrorOutOfBounds
	}
	data := make([]byte, wanted)
	if readSize, readErr := v.DataBackend.ReadAt(data, nv.Offset.ToActualOffset()+int64(offset)); readErr != nil {
		if readSize != wanted {
			return nil, fmt.Errorf("read %s.dat [%d,%d): %v",
				v.fileName, nv.Offset.ToActualOffset()+int64(offset), int(nv.Offset.ToActualOffset())+int(offset)+wanted, readErr)
		}
	} else {
		if readSize != wanted {
			return nil, fmt.Errorf("read %d, expected %d", readSize, wanted)
		}
	}

	return data, nil
}

func (v *ChunkCacheVolume) readNeedleSliceAt(data []byte, key types.NeedleId, offset uint64) (n int, err error) {
	nv, ok := v.nm.Get(key)
	if !ok {
		return 0, storage.ErrorNotFound
	}
	wanted := min(len(data), int(nv.Size)-int(offset))
	if wanted < 0 {
		// should never happen, but better than panicking
		return 0, ErrorOutOfBounds
	}
	if n, err = v.DataBackend.ReadAt(data, nv.Offset.ToActualOffset()+int64(offset)); err != nil {
		if n != wanted {
			return n, fmt.Errorf("read %s.dat [%d,%d): %v",
				v.fileName, nv.Offset.ToActualOffset()+int64(offset), int(nv.Offset.ToActualOffset())+int(offset)+wanted, err)
		}
	} else {
		if n != wanted {
			return n, fmt.Errorf("read %d, expected %d", n, wanted)
		}
	}

	return n, nil
}

func (v *ChunkCacheVolume) WriteNeedle(key types.NeedleId, data []byte) error {

	offset := v.fileSize

	written, err := v.DataBackend.WriteAt(data, offset)
	if err != nil {
		return err
	} else if written != len(data) {
		return fmt.Errorf("partial written %d, expected %d", written, len(data))
	}

	v.fileSize += int64(written)
	extraSize := written % types.NeedlePaddingSize
	if extraSize != 0 {
		v.DataBackend.WriteAt(v.smallBuffer[:types.NeedlePaddingSize-extraSize], offset+int64(written))
		v.fileSize += int64(types.NeedlePaddingSize - extraSize)
	}

	if err := v.nm.Put(key, types.ToOffset(offset), types.Size(len(data))); err != nil {
		return err
	}

	return nil
}
