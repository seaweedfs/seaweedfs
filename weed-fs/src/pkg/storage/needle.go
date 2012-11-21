package storage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"pkg/util"
	"strconv"
	"strings"
)

type Needle struct {
	Cookie uint32 "random number to mitigate brute force lookups"
	Id     uint64 "needle id"
	Size   uint32 "sum of DataSize,Data,NameSize,Name,MimeSize,Mime"
	//	DataSize uint32 "Data size"
	Data []byte "The actual file data"
	//	NameSize uint16
	//	Name     []byte "maximum 256 characters"
	//	MimeSize uint16
	//	Mime     []byte "maximum 256 characters"
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
	//log.Println("uploading file " + part.FileName())
	dotIndex := strings.LastIndex(fname, ".")
	if dotIndex > 0 {
		ext := fname[dotIndex:]
		mtype := mime.TypeByExtension(ext)
		if IsGzippable(ext, mtype) {
			data = GzipData(data)
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
func (n *Needle) Append(w io.Writer) uint32 {
	header := make([]byte, 16)
	util.Uint32toBytes(header[0:4], n.Cookie)
	util.Uint64toBytes(header[4:12], n.Id)
	n.Size = uint32(len(n.Data))
	util.Uint32toBytes(header[12:16], n.Size)
	w.Write(header)
	w.Write(n.Data)
	rest := 8 - ((n.Size + 16 + 4) % 8)
	util.Uint32toBytes(header[0:4], n.Checksum.Value())
	w.Write(header[0 : rest+4])
	return n.Size
}
func (n *Needle) Read(r io.Reader, size uint32, version Version) (int, error) {
	bytes := make([]byte, size+16+4)
	ret, e := r.Read(bytes)
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Id = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:16])
	n.Data = bytes[16 : 16+size]
	checksum := util.BytesToUint32(bytes[16+size : 16+size+4])
	if checksum != NewCRC(n.Data).Value() {
		return 0, errors.New("CRC error! Data On Disk Corrupted!")
	}
	return ret, e
}
func ReadNeedle(r *os.File) (*Needle, uint32) {
	n := new(Needle)
	bytes := make([]byte, 16)
	count, e := r.Read(bytes)
	if count <= 0 || e != nil {
		return nil, 0
	}
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Id = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:16])
	rest := 8 - ((n.Size + 16 + 4) % 8)
	return n, n.Size + 4 + rest
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
