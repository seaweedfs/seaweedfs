package storage

import (
	"sort"

	"io"
	"os"

	"sync"

	"github.com/chrislusf/seaweedfs/go/util"
	"io/ioutil"
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
		return /*v.Offset <= offset &&*/ v.Offset+int64(v.Size) > offset
	})
}

type CleanDataReader struct {
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
				//mark old needle file as dirty data
				if int64(nv.Size)-NeedleHeaderSize > 0 {
					dirtys = append(dirtys, DirtyData{
						Offset: int64(nv.Offset)*8 + NeedleHeaderSize,
						Size:   nv.Size - NeedleHeaderSize,
					})
				}
			}
			m.Delete(k)
		}
	}
	dirtys.Sort()
	return dirtys
}

func (cf *CleanDataReader) Seek(offset int64, whence int) (int64, error) {
	off, e := cf.DataFile.Seek(0, 1)
	if e != nil {
		return 0, nil
	}
	if off != offset {
		cf.Close()
	}
	return cf.DataFile.Seek(offset, whence)
}

func (cf *CleanDataReader) WriteTo(w io.Writer) (written int64, err error) {
	off, e := cf.DataFile.Seek(0, 1)
	if e != nil {
		return 0, nil
	}
	const ZeroBufSize = 32 * 1024
	zeroBuf := make([]byte, ZeroBufSize)
	dirtyIndex := cf.Dirtys.Search(off)
	var nextDirty *DirtyData
	if dirtyIndex < len(cf.Dirtys) {
		nextDirty = &cf.Dirtys[dirtyIndex]
		if nextDirty.Offset+int64(nextDirty.Size) < off {
			nextDirty = nil
		}
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
			if dirtyIndex < len(cf.Dirtys) {
				nextDirty = &cf.Dirtys[dirtyIndex]
			} else {
				nextDirty = nil
			}
			if _, e = cf.DataFile.Seek(off, 0); e != nil {
				return
			}
		} else {
			var n, sz int64
			if nextDirty != nil {
				sz = nextDirty.Offset - off
			}
			if sz > 0 {
				if n, e = io.CopyN(w, cf.DataFile, sz); e != nil {
					return
				}
			} else {
				if n, e = io.Copy(w, cf.DataFile); e != nil {
					return
				}
			}
			off += n
			written += n
		}
	}
	return
}

func (cf *CleanDataReader) ReadAt(p []byte, off int64) (n int, err error) {
	cf.Seek(off, 0)
	return cf.Read(p)
}

func (cf *CleanDataReader) Read(p []byte) (int, error) {
	return cf.getPipeReader().Read(p)
}

func (cf *CleanDataReader) Close() (e error) {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()
	cf.closePipe()
	return cf.DataFile.Close()
}

func (cf *CleanDataReader) closePipe() (e error) {
	if cf.pr != nil {
		if err := cf.pr.Close(); err != nil {
			e = err
		}
	}
	cf.pr = nil
	if cf.pw != nil {
		if err := cf.pw.Close(); err != nil {
			e = err
		}
	}
	cf.pw = nil
	return e
}

func (cf *CleanDataReader) getPipeReader() io.Reader {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()
	if cf.pr != nil && cf.pw != nil {
		return cf.pr
	}
	cf.closePipe()
	cf.pr, cf.pw = io.Pipe()
	go func(pw *io.PipeWriter) {
		_, e := cf.WriteTo(pw)
		pw.CloseWithError(e)
	}(cf.pw)
	return cf.pr
}
