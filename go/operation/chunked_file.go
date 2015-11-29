package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/chrislusf/seaweedfs/go/util"
)

var ErrOutOfRange = errors.New("Out of Range")

type ChunkInfo struct {
	Fid    string `json:"fid,omitempty"`
	Offset uint64 `json:"offset,omitempty"`
	Size   uint32 `json:"size,omitempty"`
}

type ChunkList []*ChunkInfo

type ChunkedFile struct {
	Name   string    `json:"name,omitempty"`
	Mime   string    `json:"mime,omitempty"`
	Size   uint64    `json:"size,omitempty"`
	Chunks ChunkList `json:"chunks,omitempty"`

	master string `json:"-"`
}

func (s ChunkList) Len() int           { return len(s) }
func (s ChunkList) Less(i, j int) bool { return s[i].Offset < s[j].Offset }
func (s ChunkList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func NewChunkedNeedle(buffer []byte, master string) (*ChunkedFile, error) {
	c := ChunkedFile{}

	if e := json.Unmarshal(buffer, c); e != nil {
		return nil, e
	}
	sort.Sort(c.Chunks)
	c.master = master
	return &c, nil
}

func (c *ChunkedFile) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func copyChunk(fileUrl string, w io.Writer, startOffset, size int64) (written int64, e error) {
	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return written, err
	}
	if startOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", startOffset))
	}

	resp, err := util.Do(req)
	if err != nil {
		return written, err
	}
	defer resp.Close()
	if startOffset > 0 && resp.StatusCode != 206 {
		return written, fmt.Errorf("Cannot Read Needle Position: %d [%s]", startOffset, fileUrl)
	}

	if size > 0 {
		return io.CopyN(w, resp, size)
	} else {
		return io.Copy(w, resp)
	}
}

func (c *ChunkedFile) WriteBuffer(w io.Writer, offset, size int64) (written int64, e error) {
	if offset >= c.Size || offset+size > c.Size {
		return written, ErrOutOfRange
	}
	chunkIndex := -1
	chunkStartOffset := 0
	for i, ci := range c.Chunks {
		if offset >= ci.Offset && offset < ci.Offset+ci.Size {
			chunkIndex = i
			chunkStartOffset = offset - ci.Offset
			break
		}
	}
	if chunkIndex < 0 {
		return written, ErrOutOfRange
	}
	for ; chunkIndex < c.Chunks.Len(); chunkIndex++ {
		ci := c.Chunks[chunkIndex]
		fileUrl, lookupError := LookupFileId(c.master, ci.Fid)
		if lookupError != nil {
			return written, lookupError
		}
		rsize := 0
		if size > 0 {
			rsize = size - written
		}
		if n, e := copyChunk(fileUrl, w, chunkStartOffset, rsize); e != nil {
			return written, e
		} else {
			written += n
		}

		if size > 0 && written >= size {
			break
		}
		chunkStartOffset = 0
	}

	return written, nil
}

func (c *ChunkedFile) DeleteHelper() error {
	//TODO Delete all chunks
	return nil
}

func (c *ChunkedFile) StoredHelper() error {
	//TODO
	return nil
}
