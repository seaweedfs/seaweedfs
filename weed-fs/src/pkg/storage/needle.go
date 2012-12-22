package storage

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"pkg/util"
	"strconv"
	"strings"
)

const (
	NeedleHeaderSize  = 16 //should never change this
	NeedlePaddingSize = 8
)

type Needle struct {
	Cookie uint32 "random number to mitigate brute force lookups"
	Id     uint64 "needle id"
	Size   uint32 "sum of DataSize,Data,NameSize,Name,MimeSize,Mime"

	DataSize uint32 "Data size" //version2
	Data     []byte "The actual file data"
	Flags    byte   "boolean flags" //version2
	NameSize uint8  //version2
	Name     []byte "maximum 256 characters" //version2
	MimeSize uint8  //version2
	Mime     []byte "maximum 256 characters" //version2

	Checksum CRC    "CRC32 to check integrity"
	Padding  []byte "Aligned to 8 bytes"
}

func NewNeedle(r *http.Request) (n *Needle, fname string, e error) {

	n = new(Needle)
	form, fe := r.MultipartReader()
	if fe != nil {
		fmt.Println("MultipartReader [ERROR]", fe)
		e = fe
		return
	}
	part, fe := form.NextPart()
	if fe != nil {
		fmt.Println("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}
	fname = part.FileName()
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
		mtype = contentType
	}
	if IsGzippable(ext, mtype) {
		data = GzipData(data)
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
	}

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
			println("Invalid fid", fid, "length", length)
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
		println("Invalid key_hash", key_hash_string, "length:", key_hash_len, "error", khe)
		return 0, 0
	}
	key := util.BytesToUint64(key_hash_bytes[0 : key_hash_len-4])
	hash := util.BytesToUint32(key_hash_bytes[key_hash_len-4 : key_hash_len])
	return key, hash
}
