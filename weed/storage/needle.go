package storage

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/images"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"io/ioutil"
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
	Size   uint32   `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`

	DataSize     uint32 `comment:"Data size"` //version2
	Data         []byte `comment:"The actual file data"`
	Flags        byte   `comment:"boolean flags"` //version2
	NameSize     uint8  //version2
	Name         []byte `comment:"maximum 256 characters"` //version2
	MimeSize     uint8  //version2
	Mime         []byte `comment:"maximum 256 characters"` //version2
	PairsSize    uint16 //version2
	Pairs        []byte `comment:"additional name value pairs, json format, maximum 64kB"`
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk
	Ttl          *TTL

	Checksum   CRC    `comment:"CRC32 to check integrity"`
	AppendAtNs uint64 `comment:"append timestamp in nano seconds"` //version3
	Padding    []byte `comment:"Aligned to 8 bytes"`
}

func (n *Needle) String() (str string) {
	str = fmt.Sprintf("%s Size:%d, DataSize:%d, Name:%s, Mime:%s", formatNeedleIdCookie(n.Id, n.Cookie), n.Size, n.DataSize, n.Name, n.Mime)
	return
}

func ParseUpload(r *http.Request) (
	fileName string, data []byte, mimeType string, pairMap map[string]string, isGzipped bool, originalDataSize int,
	modifiedTime uint64, ttl *TTL, isChunkedFile bool, e error) {
	pairMap = make(map[string]string)
	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, PairNamePrefix) {
			pairMap[k] = v[0]
		}
	}

	if r.Method == "POST" {
		fileName, data, mimeType, isGzipped, originalDataSize, isChunkedFile, e = parseMultipart(r)
	} else {
		isGzipped = false
		mimeType = r.Header.Get("Content-Type")
		fileName = ""
		data, e = ioutil.ReadAll(r.Body)
		originalDataSize = len(data)
	}
	if e != nil {
		return
	}

	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	ttl, _ = ReadTTL(r.FormValue("ttl"))

	return
}
func CreateNeedleFromRequest(r *http.Request, fixJpgOrientation bool) (n *Needle, originalSize int, e error) {
	var pairMap map[string]string
	fname, mimeType, isGzipped, isChunkedFile := "", "", false, false
	n = new(Needle)
	fname, n.Data, mimeType, pairMap, isGzipped, originalSize, n.LastModified, n.Ttl, isChunkedFile, e = ParseUpload(r)
	if e != nil {
		return
	}
	if len(fname) < 256 {
		n.Name = []byte(fname)
		n.SetHasName()
	}
	if len(mimeType) < 256 {
		n.Mime = []byte(mimeType)
		n.SetHasMime()
	}
	if len(pairMap) != 0 {
		trimmedPairMap := make(map[string]string)
		for k, v := range pairMap {
			trimmedPairMap[k[len(PairNamePrefix):]] = v
		}

		pairs, _ := json.Marshal(trimmedPairMap)
		if len(pairs) < 65536 {
			n.Pairs = pairs
			n.PairsSize = uint16(len(pairs))
			n.SetHasPairs()
		}
	}
	if isGzipped {
		n.SetGzipped()
	}
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()
	if n.Ttl != EMPTY_TTL {
		n.SetHasTtl()
	}

	if isChunkedFile {
		n.SetIsChunkManifest()
	}

	if fixJpgOrientation {
		loweredName := strings.ToLower(fname)
		if mimeType == "image/jpeg" || strings.HasSuffix(loweredName, ".jpg") || strings.HasSuffix(loweredName, ".jpeg") {
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
			n.Id += NeedleId(d)
		} else {
			return e
		}
	}
	return err
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
