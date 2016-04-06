package storage

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	// when the remote server does not allow range requests (Accept-Ranges was not set)
	ErrRangeRequestsNotSupported = errors.New("Range requests are not supported by the remote server")
	// ErrInvalidRange is returned by Read when trying to read past the end of the file
	ErrInvalidRange = errors.New("Invalid range")
)

// seekable chunked file reader
type ChunkedFileReader struct {
	Manifest   *operation.ChunkManifest
	Master     string
	Collection string
	Store      *Store
	pos        int64
	pr         *io.PipeReader
	pw         *io.PipeWriter
	mutex      sync.Mutex
}

func (cf *ChunkedFileReader) Seek(offset int64, whence int) (int64, error) {
	var err error
	switch whence {
	case 0:
	case 1:
		offset += cf.pos
	case 2:
		offset = cf.Manifest.Size - offset
	}
	if offset > cf.Manifest.Size {
		err = ErrInvalidRange
	}
	if cf.pos != offset {
		cf.Close()
	}
	cf.pos = offset
	return cf.pos, err
}

func (cf *ChunkedFileReader) readRemoteChunkNeedle(fid string, w io.Writer, offset int64) (written int64, e error) {
	fileUrl, lookupError := operation.LookupFileId(cf.Master, fid, cf.Collection, true)
	if lookupError != nil {
		return 0, lookupError
	}

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return written, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	resp, err := util.HttpDo(req)
	if err != nil {
		return written, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusRequestedRangeNotSatisfiable:
		return written, ErrInvalidRange
	case http.StatusOK:
		if offset > 0 {
			return written, ErrRangeRequestsNotSupported
		}
	case http.StatusPartialContent:
		break
	default:
		return written, fmt.Errorf("Read chunk needle error: [%d] %s", resp.StatusCode, fileUrl)

	}
	return io.Copy(w, resp.Body)
}

func (cf *ChunkedFileReader) readLocalChunkNeedle(fid *FileId, w io.Writer, offset int64) (written int64, e error) {
	n := &Needle{
		Id:     fid.Key,
		Cookie: fid.Hashcode,
	}
	cookie := n.Cookie
	count, e := cf.Store.ReadVolumeNeedle(fid.VolumeId, n)
	if e != nil || count <= 0 {
		return 0, e
	}
	if n.Cookie != cookie {
		return 0, fmt.Errorf("read error: with unmaching cookie seen: %x expected: %x", cookie, n.Cookie)
	}
	wn, e := w.Write(n.Data[offset:])
	return int64(wn), e
}

func (cf *ChunkedFileReader) WriteTo(w io.Writer) (n int64, err error) {
	cm := cf.Manifest
	chunkIndex := -1
	chunkStartOffset := int64(0)
	for i, ci := range cm.Chunks {
		if cf.pos >= ci.Offset && cf.pos < ci.Offset+ci.Size {
			chunkIndex = i
			chunkStartOffset = cf.pos - ci.Offset
			break
		}
	}
	if chunkIndex < 0 {
		return n, ErrInvalidRange
	}
	for ; chunkIndex < cm.Chunks.Len(); chunkIndex++ {
		ci := cm.Chunks[chunkIndex]
		fid, e := ParseFileId(ci.Fid)
		if e != nil {
			return n, e
		}
		var wn int64
		if cf.Store != nil && cf.Store.HasVolume(fid.VolumeId) {
			wn, e = cf.readLocalChunkNeedle(fid, w, chunkStartOffset)
		} else {
			wn, e = cf.readRemoteChunkNeedle(ci.Fid, w, chunkStartOffset)
		}

		if e != nil {
			return n, e
		} else {
			n += wn
			cf.pos += wn
		}

		chunkStartOffset = 0
	}
	return n, nil
}

func (cf *ChunkedFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	cf.Seek(off, 0)
	return cf.Read(p)
}

func (cf *ChunkedFileReader) Read(p []byte) (int, error) {
	return cf.getPipeReader().Read(p)
}

func (cf *ChunkedFileReader) Close() (e error) {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()
	return cf.closePipe()
}

func (cf *ChunkedFileReader) closePipe() (e error) {
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

func (cf *ChunkedFileReader) getPipeReader() io.Reader {
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
