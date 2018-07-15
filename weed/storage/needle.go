package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/operation"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
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
	Flags        byte   `comment:"boolean flags"`          //version2
	NameSize     uint8                                     //version2
	Name         []byte `comment:"maximum 256 characters"` //version2
	MimeSize     uint8                                     //version2
	Mime         []byte `comment:"maximum 256 characters"` //version2
	PairsSize    uint16                                    //version2
	Pairs        []byte `comment:"additional name value pairs, json format, maximum 64kB"`
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk
	Ttl          *TTL

	Checksum CRC    `comment:"CRC32 to check integrity"`
	Padding  []byte `comment:"Aligned to 8 bytes"`
}

func (n *Needle) String() (str string) {
	str = fmt.Sprintf("Cookie:%d, Id:%d, Size:%d, DataSize:%d, Name: %s, Mime: %s", n.Cookie, n.Id, n.Size, n.DataSize, n.Name, n.Mime)
	return
}

func ParseUpload(r *http.Request) (
	fileName string, data []byte, mimeType string, pairMap map[string]string, isGzipped bool,
	modifiedTime uint64, ttl *TTL, isChunkedFile bool, e error) {
	pairMap = make(map[string]string)
	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, PairNamePrefix) {
			pairMap[k] = v[0]
		}
	}

	form, fe := r.MultipartReader()
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}

	//first multi-part item
	part, fe := form.NextPart()
	if fe != nil {
		glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}

	fileName = part.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}

	data, e = ioutil.ReadAll(part)
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}

	//if the filename is empty string, do a search on the other multi-part items
	for fileName == "" {
		part2, fe := form.NextPart()
		if fe != nil {
			break // no more or on error, just safely break
		}

		fName := part2.FileName()

		//found the first <file type> multi-part has filename
		if fName != "" {
			data2, fe2 := ioutil.ReadAll(part2)
			if fe2 != nil {
				glog.V(0).Infoln("Reading Content [ERROR]", fe2)
				e = fe2
				return
			}

			//update
			data = data2
			fileName = path.Base(fName)
			break
		}
	}

	isChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))

	if !isChunkedFile {

		dotIndex := strings.LastIndex(fileName, ".")
		ext, mtype := "", ""
		if dotIndex > 0 {
			ext = strings.ToLower(fileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}
		contentType := part.Header.Get("Content-Type")
		if contentType != "" && mtype != contentType {
			mimeType = contentType //only return mime type if not deductable
			mtype = contentType
		}

		if part.Header.Get("Content-Encoding") == "gzip" {
			isGzipped = true
		} else if operation.IsGzippable(ext, mtype) {
			if data, e = operation.GzipData(data); e != nil {
				return
			}
			isGzipped = true
		}
		if ext == ".gz" {
			if strings.HasSuffix(fileName, ".css.gz") ||
				strings.HasSuffix(fileName, ".html.gz") ||
				strings.HasSuffix(fileName, ".txt.gz") ||
				strings.HasSuffix(fileName, ".js.gz") {
				fileName = fileName[:len(fileName)-3]
				isGzipped = true
			}
		}
	}
	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	ttl, _ = ReadTTL(r.FormValue("ttl"))

	return
}
func NewNeedle(r *http.Request, fixJpgOrientation bool) (n *Needle, e error) {
	var pairMap map[string]string
	fname, mimeType, isGzipped, isChunkedFile := "", "", false, false
	n = new(Needle)
	fname, n.Data, mimeType, pairMap, isGzipped, n.LastModified, n.Ttl, isChunkedFile, e = ParseUpload(r)
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
		fid = r.URL.Path[commaSep+1: dotSep]
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
		return 0, 0, fmt.Errorf("KeyHash is too short.")
	}
	if len(key_hash_string) > (NeedleIdSize+CookieSize)*2 {
		return 0, 0, fmt.Errorf("KeyHash is too long.")
	}
	split := len(key_hash_string) - CookieSize*2
	needleId, err := ParseNeedleId(key_hash_string[:split])
	if err != nil {
		return 0, 0, fmt.Errorf("Parse needleId error: %v", err)
	}
	cookie, err := ParseCookie(key_hash_string[split:])
	if err != nil {
		return 0, 0, fmt.Errorf("Parse cookie error: %v", err)
	}
	return needleId, cookie, nil
}

func (n *Needle) LastModifiedString() string {
	return time.Unix(int64(n.LastModified), 0).Format("2006-01-02T15:04:05")
}
