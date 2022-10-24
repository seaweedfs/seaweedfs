package needle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/images"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	NeedleChecksumSize = 4
	PairNamePrefix     = "Seaweed-"
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Cookie Cookie   `comment:"random number to mitigate brute force lookups"`
	Id     NeedleId `comment:"needle id"`
	Size   Size     `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`

	DataSize     uint32 `comment:"Data size"` //version2
	Data         []byte `comment:"The actual file data"`
	Flags        byte   `comment:"boolean flags"` //version2
	NameSize     uint8  //version2
	Name         []byte `comment:"maximum 255 characters"` //version2
	MimeSize     uint8  //version2
	Mime         []byte `comment:"maximum 255 characters"` //version2
	PairsSize    uint16 //version2
	Pairs        []byte `comment:"additional name value pairs, json format, maximum 64kB"`
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk
	Ttl          *TTL

	Checksum   CRC    `comment:"CRC32 to check integrity"`
	AppendAtNs uint64 `comment:"append timestamp in nano seconds"` //version3
	Padding    []byte `comment:"Aligned to 8 bytes"`
}

func (n *Needle) String() (str string) {
	str = fmt.Sprintf("%s Size:%d, DataSize:%d, Name:%s, Mime:%s Compressed:%v", formatNeedleIdCookie(n.Id, n.Cookie), n.Size, n.DataSize, n.Name, n.Mime, n.IsCompressed())
	return
}

func CreateNeedleFromRequest(r *http.Request, fixJpgOrientation bool, sizeLimit int64, bytesBuffer *bytes.Buffer) (n *Needle, originalSize int, contentMd5 string, e error) {
	n = new(Needle)
	pu, e := ParseUpload(r, sizeLimit, bytesBuffer)
	if e != nil {
		return
	}
	n.Data = pu.Data
	originalSize = pu.OriginalDataSize
	n.LastModified = pu.ModifiedTime
	n.Ttl = pu.Ttl
	contentMd5 = pu.ContentMd5

	if len(pu.FileName) < 256 {
		n.Name = []byte(pu.FileName)
		n.SetHasName()
	}
	if len(pu.MimeType) < 256 {
		n.Mime = []byte(pu.MimeType)
		n.SetHasMime()
	}
	if len(pu.PairMap) != 0 {
		trimmedPairMap := make(map[string]string)
		for k, v := range pu.PairMap {
			trimmedPairMap[k[len(PairNamePrefix):]] = v
		}

		pairs, _ := json.Marshal(trimmedPairMap)
		if len(pairs) < 65536 {
			n.Pairs = pairs
			n.PairsSize = uint16(len(pairs))
			n.SetHasPairs()
		}
	}
	if pu.IsGzipped {
		// println(r.URL.Path, "is set to compressed", pu.FileName, pu.IsGzipped, "dataSize", pu.OriginalDataSize)
		n.SetIsCompressed()
	}
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()
	if n.Ttl != EMPTY_TTL {
		n.SetHasTtl()
	}

	if pu.IsChunkedFile {
		n.SetIsChunkManifest()
	}

	if fixJpgOrientation {
		loweredName := strings.ToLower(pu.FileName)
		if pu.MimeType == "image/jpeg" || strings.HasSuffix(loweredName, ".jpg") || strings.HasSuffix(loweredName, ".jpeg") {
			n.Data = images.FixJpgOrientation(n.Data)
		}
	}

	n.Checksum = NewCRC(n.Data)

	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	e = n.ParsePath(fid)

	return
}
func (n *Needle) ParsePath(fid string) (err error) {
	length := len(fid)
	if length <= CookieSize*2 {
		return fmt.Errorf("Invalid fid: %s", fid)
	}
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	n.Id, n.Cookie, err = ParseNeedleIdCookie(fid)
	if err != nil {
		return err
	}
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Id += Uint64ToNeedleId(d)
		} else {
			return e
		}
	}
	return err
}

func GetAppendAtNs(volumeLastAppendAtNs uint64) uint64 {
	return max(uint64(time.Now().UnixNano()), volumeLastAppendAtNs+1)
}

func (n *Needle) UpdateAppendAtNs(volumeLastAppendAtNs uint64) {
	n.AppendAtNs = max(uint64(time.Now().UnixNano()), volumeLastAppendAtNs+1)
}

func ParseNeedleIdCookie(key_hash_string string) (NeedleId, Cookie, error) {
	if len(key_hash_string) <= CookieSize*2 {
		return NeedleIdEmpty, 0, fmt.Errorf("KeyHash is too short.")
	}
	if len(key_hash_string) > (NeedleIdSize+CookieSize)*2 {
		return NeedleIdEmpty, 0, fmt.Errorf("KeyHash is too long.")
	}
	split := len(key_hash_string) - CookieSize*2
	needleId, err := ParseNeedleId(key_hash_string[:split])
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("Parse needleId error: %v", err)
	}
	cookie, err := ParseCookie(key_hash_string[split:])
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("Parse cookie error: %v", err)
	}
	return needleId, cookie, nil
}

func (n *Needle) LastModifiedString() string {
	return time.Unix(int64(n.LastModified), 0).Format("2006-01-02T15:04:05")
}

func max(x, y uint64) uint64 {
	if x <= y {
		return y
	}
	return x
}
