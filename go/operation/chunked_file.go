package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"

	"sync"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/util"
)

var (
	// when the remote server does not allow range requests (Accept-Ranges was not set)
	ErrRangeRequestsNotSupported = errors.New("Range requests are not supported by the remote server")
	// ErrInvalidRange is returned by Read when trying to read past the end of the file
	ErrInvalidRange = errors.New("Invalid range")
)

type ChunkInfo struct {
	Fid    string `json:"fid,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Size   int64  `json:"size,omitempty"`
}

type ChunkList []*ChunkInfo

type ChunkManifest struct {
	Name      string    `json:"name,omitempty"`
	Mime      string    `json:"mime,omitempty"`
	Size      int64     `json:"size,omitempty"`
	Chunks    ChunkList `json:"chunks,omitempty"`
}

// seekable chunked file reader
type ChunkedFileReader struct {
	Manifest *ChunkManifest
	Master   string
	pos      int64
	pr       *io.PipeReader
	pw       *io.PipeWriter
	mutex    sync.Mutex
}

func (s ChunkList) Len() int           { return len(s) }
func (s ChunkList) Less(i, j int) bool { return s[i].Offset < s[j].Offset }
func (s ChunkList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func LoadChunkedManifest(buffer []byte) (*ChunkManifest, error) {
	cm := ChunkManifest{}
	if e := json.Unmarshal(buffer, cm); e != nil {
		return nil, e
	}
	sort.Sort(cm.Chunks)
	return &cm, nil
}

func (cm *ChunkManifest) GetData() ([]byte, error) {
	return json.Marshal(cm)
}

func (cm *ChunkManifest) DeleteChunks(master string) error {
	fileIds := make([]string, 0, len(cm.Chunks))
	for _, ci := range cm.Chunks {
		fileIds = append(fileIds, ci.Fid)
	}
	results, e := DeleteFiles(master, fileIds)
	if e != nil {
		return e
	}
	deleteError := 0
	for _, ret := range results.Results {
		if ret.Error != "" {
			deleteError++
			glog.V(0).Infoln("delete error:", ret.Error, ret.Fid)
		}
	}
	if deleteError > 0 {
		return errors.New("Not all chunks deleted.")
	}
	return nil
}

//func (cm *ChunkManifest) StoredHelper() error {
//	return nil
//}

func httpRangeDownload(fileUrl string, w io.Writer, offset int64) (written int64, e error) {
	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return written, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	resp, err := util.Do(req)
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
		return written, fmt.Errorf("Read Needle http error: [%d] %s", resp.StatusCode, fileUrl)

	}
	return io.Copy(w, resp.Body)
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
		// if we need read date from local volume server first?
		fileUrl, lookupError := LookupFileId(cf.Master, ci.Fid)
		if lookupError != nil {
			return n, lookupError
		}
		if wn, e := httpRangeDownload(fileUrl, w, chunkStartOffset); e != nil {
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
