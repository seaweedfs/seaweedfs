package storage

import (
	"io"
	"os"
	"sort"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/util"
)

type DirtyData struct {
	Offset int64  `comment:"Dirty data start offset"`
	Size   uint32 `comment:"Size of the dirty data"`
}

type DirtyDatas []DirtyData

func (s DirtyDatas) Len() int           { return len(s) }
func (s DirtyDatas) Less(i, j int) bool { return s[i].Offset < s[j].Offset }
func (s DirtyDatas) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s DirtyDatas) Sort()              { sort.Sort(s) }
func (s DirtyDatas) Search(offset int64) int {
	return sort.Search(len(s), func(i int) bool {
		v := &s[i]
		return v.Offset+int64(v.Size) > offset
	})
}

type PureReader struct {
	Dirtys   DirtyDatas
	DataFile *os.File
	pr       *io.PipeReader
	pw       *io.PipeWriter
	mutex    sync.Mutex
}

func ScanDirtyData(indexFileContent []byte) (dirtys DirtyDatas) {
	m := NewCompactMap()
	for i := 0; i+16 <= len(indexFileContent); i += 16 {
		bytes := indexFileContent[i : i+16]
		key := util.BytesToUint64(bytes[:8])
		offset := util.BytesToUint32(bytes[8:12])
		size := util.BytesToUint32(bytes[12:16])
		k := Key(key)
		if offset != 0 && size != 0 {
			m.Set(k, offset, size)
		} else {
			if nv, ok := m.Get(k); ok {
				//mark old needle data as dirty data
				if int64(nv.Size)-NeedleHeaderSize > 0 {
					dirtys = append(dirtys, DirtyData{
						Offset: int64(nv.Offset)*8 + NeedleHeaderSize,
						Size:   nv.Size,
					})
				}
			}
			m.Delete(k)
		}
	}
	dirtys.Sort()
	return dirtys
}

func (cr *PureReader) Seek(offset int64, whence int) (int64, error) {
	oldOff, e := cr.DataFile.Seek(0, 1)
	if e != nil {
		return 0, e
	}
	newOff, e := cr.DataFile.Seek(offset, whence)
	if e != nil {
		return 0, e
	}
	if oldOff != newOff {
		cr.closePipe(true)
	}
	return newOff, nil
}

func (cr *PureReader) Size() (int64, error) {
	fi, e := cr.DataFile.Stat()
	if e != nil {
		return 0, e
	}
	return fi.Size(), nil
}

func (cdr *PureReader) WriteTo(w io.Writer) (written int64, err error) {
	off, e := cdr.DataFile.Seek(0, 1)
	if e != nil {
		return 0, nil
	}
	const ZeroBufSize = 32 * 1024
	zeroBuf := make([]byte, ZeroBufSize)
	dirtyIndex := cdr.Dirtys.Search(off)
	var nextDirty *DirtyData
	if dirtyIndex < len(cdr.Dirtys) {
		nextDirty = &cdr.Dirtys[dirtyIndex]
	}
	for {
		if nextDirty != nil && off >= nextDirty.Offset && off < nextDirty.Offset+int64(nextDirty.Size) {
			sz := nextDirty.Offset + int64(nextDirty.Size) - off
			for sz > 0 {
				mn := int64(ZeroBufSize)
				if mn > sz {
					mn = sz
				}
				var n int
				if n, e = w.Write(zeroBuf[:mn]); e != nil {
					return
				}
				written += int64(n)
				sz -= int64(n)
				off += int64(n)
			}
			dirtyIndex++
			if dirtyIndex < len(cdr.Dirtys) {
				nextDirty = &cdr.Dirtys[dirtyIndex]
			} else {
				nextDirty = nil
			}
			if _, e = cdr.DataFile.Seek(off, 0); e != nil {
				return
			}
		} else {
			var n, sz int64
			if nextDirty != nil {
				sz = nextDirty.Offset - off
			}
			if sz <= 0 {
				// copy until eof
				n, e = io.Copy(w, cdr.DataFile)
				written += n
				return
			}
			if n, e = io.CopyN(w, cdr.DataFile, sz); e != nil {
				return
			}
			off += n
			written += n
		}
	}
}

func (cr *PureReader) ReadAt(p []byte, off int64) (n int, err error) {
	cr.Seek(off, 0)
	return cr.Read(p)
}

func (cr *PureReader) Read(p []byte) (int, error) {
	return cr.getPipeReader().Read(p)
}

func (cr *PureReader) Close() (e error) {
	cr.closePipe(true)
	return cr.DataFile.Close()
}

func (cr *PureReader) closePipe(lock bool) (e error) {
	if lock {
		cr.mutex.Lock()
		defer cr.mutex.Unlock()
	}
	if cr.pr != nil {
		if err := cr.pr.Close(); err != nil {
			e = err
		}
	}
	cr.pr = nil
	if cr.pw != nil {
		if err := cr.pw.Close(); err != nil {
			e = err
		}
	}
	cr.pw = nil
	return e
}

func (cr *PureReader) getPipeReader() io.Reader {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()
	if cr.pr != nil && cr.pw != nil {
		return cr.pr
	}
	cr.closePipe(false)
	cr.pr, cr.pw = io.Pipe()
	go func(pw *io.PipeWriter) {
		_, e := cr.WriteTo(pw)
		pw.CloseWithError(e)
	}(cr.pw)
	return cr.pr
}

func (v *Volume) GetVolumeCleanReader() (cr *PureReader, err error) {
	var dirtys DirtyDatas
	if indexData, e := v.nm.IndexFileContent(); e != nil {
		return nil, err
	} else {
		dirtys = ScanDirtyData(indexData)
	}
	dataFile, e := os.Open(v.FileName() + ".dat")

	if e != nil {
		return nil, e
	}
	cr = &PureReader{
		Dirtys:   dirtys,
		DataFile: dataFile,
	}
	return
}
