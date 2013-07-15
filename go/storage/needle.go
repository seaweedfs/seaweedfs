package storage

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	NeedleHeaderSize   = 16 //should never change this
	NeedlePaddingSize  = 8
	NeedleChecksumSize = 4
)

type Needle struct {
	Cookie uint32 `comment:"random number to mitigate brute force lookups"`
	Id     uint64 `comment:"needle id"`
	Size   uint32 `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`

	DataSize     uint32 `comment:"Data size"` //version2
	Data         []byte `comment:"The actual file data"`
	Flags        byte   `comment:"boolean flags"` //version2
	NameSize     uint8  //version2
	Name         []byte `comment:"maximum 256 characters"` //version2
	MimeSize     uint8  //version2
	Mime         []byte `comment:"maximum 256 characters"` //version2
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk

	Checksum CRC    `comment:"CRC32 to check integrity"`
	Padding  []byte `comment:"Aligned to 8 bytes"`
}

func NewNeedle(r *http.Request) (n *Needle, e error) {

	n = new(Needle)
	form, fe := r.MultipartReader()
	if fe != nil {
		log.Println("MultipartReader [ERROR]", fe)
		e = fe
		return
	}
	part, fe := form.NextPart()
	if fe != nil {
		log.Println("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}
	fname := part.FileName()
	if fname != "" {
		fname = path.Base(part.FileName())
	} else {
		e = errors.New("No file found!")
		return
	}
	data, _ := ioutil.ReadAll(part)
	dotIndex := strings.LastIndex(fname, ".")
	ext, mtype := "", ""
	if dotIndex > 0 {
		ext = fname[dotIndex:]
		mtype = mime.TypeByExtension(ext)
	}
	contentType := part.Header.Get("Content-Type")
	if contentType != "" && mtype != contentType && len(contentType) < 256 {
		n.Mime = []byte(contentType)
		n.SetHasMime()
		mtype = contentType
	}
	if part.Header.Get("Content-Encoding") == "gzip" {
		n.SetGzipped()
	} else if IsGzippable(ext, mtype) {
		if data, e = GzipData(data); e != nil {
			return
		}
		n.SetGzipped()
	}
	if ext == ".gz" {
		n.SetGzipped()
	}
	if len(fname) < 256 {
		if strings.HasSuffix(fname, ".gz") {
			n.Name = []byte(fname[:len(fname)-3])
		} else {
			n.Name = []byte(fname)
		}
		n.SetHasName()
	}
	var parseError error
	if n.LastModified, parseError = strconv.ParseUint(r.FormValue("ts"), 10, 64); parseError != nil {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()

	n.Data = data
	n.Checksum = NewCRC(data)

	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	n.ParsePath(fid)

	return
}
func (n *Needle) ParsePath(fid string) {
	length := len(fid)
	if length <= 8 {
		if length > 0 {
			log.Println("Invalid fid", fid, "length", length)
		}
		return
	}
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	n.Id, n.Cookie = ParseKeyHash(fid)
	if delta != "" {
		d, e := strconv.ParseUint(delta, 10, 64)
		if e == nil {
			n.Id += d
		}
	}
}

func ParseKeyHash(key_hash_string string) (uint64, uint32) {
	key_hash_bytes, khe := hex.DecodeString(key_hash_string)
	key_hash_len := len(key_hash_bytes)
	if khe != nil || key_hash_len <= 4 {
		log.Println("Invalid key_hash", key_hash_string, "length:", key_hash_len, "error", khe)
		return 0, 0
	}
	key := util.BytesToUint64(key_hash_bytes[0 : key_hash_len-4])
	hash := util.BytesToUint32(key_hash_bytes[key_hash_len-4 : key_hash_len])
	return key, hash
}
