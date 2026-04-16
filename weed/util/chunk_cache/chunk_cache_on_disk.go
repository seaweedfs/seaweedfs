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
	fn := v.fileName + ".dat"
	err := os.Truncate(fn, 0)
	if err != nil {
		glog.Errorf("ChunkCacheVolume.doReset: truncate %q failed: %s", fn, err)
	}
	fn = v.fileName + ".idx"
	err = os.Truncate(fn, 0)
	if err != nil {
		glog.Errorf("ChunkCacheVolume.doReset: truncate %q failed: %s", fn, err)
	}
	fn = v.fileName + ".ldb"
	err = os.RemoveAll(fn)
	if err != nil {
		glog.Errorf("ChunkCacheVolume.doReset: remove %q failed: %s", fn, err)
	} else {
		glog.V(4).Infof("cache removed %s", fn)
	}
}

func (v *ChunkCacheVolume) Reset() (*ChunkCacheVolume, error) {
	v.doReset()
	return LoadOrCreateChunkCacheVolume(v.fileName, v.sizeLimit)
}

// dropReadCache advises the kernel to drop page cache for the byte range
// just read. This is best-effort; failures are logged at V(4).
func (v *ChunkCacheVolume) dropReadCache(offset int64, length int64) {
	type fdProvider interface {
		Fd() uintptr
	}
	if fp, ok := v.DataBackend.(fdProvider); ok {
		fd := int(fp.Fd())
		if err := util.DropOSPageCache(fd, offset, length); err != nil {
			glog.V(4).Infof("fadvise DONTNEED %s offset %d len %d: %v", v.fileName, offset, length, err)
		}
	}
}

func (v *ChunkCacheVolume) GetNeedle(key types.NeedleId) ([]byte, error) {

	nv, ok := v.nm.Get(key)
	if !ok {
		return nil, storage.ErrorNotFound
	}
	data := make([]byte, nv.Size)
	readOffset := nv.Offset.ToActualOffset()
	if readSize, readErr := v.DataBackend.ReadAt(data, readOffset); readErr != nil {
		if readSize != int(nv.Size) {
			return nil, fmt.Errorf("read %s.dat [%d,%d): %v",
				v.fileName, readOffset, readOffset+int64(nv.Size), readErr)
		}
	} else {
		if readSize != int(nv.Size) {
			return nil, fmt.Errorf("read %d, expected %d", readSize, nv.Size)
		}
	}

	v.dropReadCache(readOffset, int64(nv.Size))
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
	readOffset := nv.Offset.ToActualOffset() + int64(offset)
	var readSize int
	var readErr error
	if readSize, readErr = v.DataBackend.ReadAt(data, readOffset); readErr != nil {
		if readSize != wanted {
			return nil, fmt.Errorf("read %s.dat [%d,%d): %v",
				v.fileName, readOffset, int64(readOffset)+int64(wanted), readErr)
		}
	} else {
		if readSize != wanted {
			return nil, fmt.Errorf("read %d, expected %d", readSize, wanted)
		}
	}
	if readErr != nil && readSize == wanted {
		readErr = nil
	}
	if readSize > 0 {
		v.dropReadCache(readOffset, int64(readSize))
	}
	return data, readErr
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
	readOffset := nv.Offset.ToActualOffset() + int64(offset)
	if n, err = v.DataBackend.ReadAt(data, readOffset); err != nil {
		if n != wanted {
			return n, fmt.Errorf("read %s.dat [%d,%d): %v",
				v.fileName, readOffset, int64(readOffset)+int64(wanted), err)
		}
	} else {
		if n != wanted {
			return n, fmt.Errorf("read %d, expected %d", n, wanted)
		}
	}
	if err != nil && n == wanted {
		err = nil
	}
	if n > 0 {
		v.dropReadCache(readOffset, int64(n))
	}
	return n, err
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
		_, err = v.DataBackend.WriteAt(v.smallBuffer[:types.NeedlePaddingSize-extraSize], offset+int64(written))
		if err != nil {
			return err
		}
		v.fileSize += int64(types.NeedlePaddingSize - extraSize)
	}

	if err := v.nm.Put(key, types.ToOffset(offset), types.Size(len(data))); err != nil {
		return err
	}

	return nil
}
