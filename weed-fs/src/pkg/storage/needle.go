package storage

import (
    "encoding/hex"
	"io"
	"io/ioutil"
	"http"
	"log"
	"os"
	"strings"
)

type Needle struct {
	Cookie   uint32 "random number to mitigate brute force lookups"
	Key      uint64 "file id"
	Size     uint32 "Data size"
	Data     []byte "The actual file data"
	Checksum int32  "CRC32 to check integrity"
	Padding  []byte "Aligned to 8 bytes"
}

func NewNeedle(r *http.Request) (n *Needle) {
	n = new(Needle)
	form, fe := r.MultipartReader()
	if fe != nil {
		log.Fatalf("MultipartReader [ERROR] %s\n", fe)
	}
	part, _ := form.NextPart()
	data, _ := ioutil.ReadAll(part)
	n.Data = data

    commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1:dotSep]
	}

	n.ParsePath(fid)

	return
}
func (n *Needle) ParsePath(fid string) {
	length := len(fid)
	if length <= 8 {
		log.Println("Invalid fid", fid, "length", length)
		return
	}
	n.Key, n.Cookie = ParseKeyHash(fid)
}
func (n *Needle) Append(w io.Writer) (uint32){
	header := make([]byte, 16)
	Uint32toBytes(header[0:4], n.Cookie)
	Uint64toBytes(header[4:12], n.Key)
	n.Size = uint32(len(n.Data))
	Uint32toBytes(header[12:16], n.Size)
	w.Write(header)
	w.Write(n.Data)
	rest := 8 - ((n.Size + 16 + 4) % 8)
	Uint32toBytes(header[0:4], uint32(n.Checksum))
	w.Write(header[0 : rest+4])
	return n.Size
}
func (n *Needle) Read(r io.Reader, size uint32)(int, os.Error) {
	bytes := make([]byte, size+16+4)
	ret, e := r.Read(bytes)
	n.Cookie = BytesToUint32(bytes[0:4])
	n.Key = BytesToUint64(bytes[4:12])
	n.Size = BytesToUint32(bytes[12:16])
	n.Data = bytes[16 : 16+size]
	n.Checksum = int32(BytesToUint32(bytes[16+size : 16+size+4]))
	return ret, e
}
func ReadNeedle(r *os.File) (*Needle, uint32) {
    n := new(Needle)
    bytes := make([]byte, 16)
    count, e := r.Read(bytes)
    if count<=0 || e!=nil {
        return nil, 0
    }
    n.Cookie = BytesToUint32(bytes[0:4])
    n.Key = BytesToUint64(bytes[4:12])
    n.Size = BytesToUint32(bytes[12:16])
    rest := 8 - ((n.Size + 16 + 4) % 8)
    r.Seek(int64(n.Size+4+rest), 1)
    return n, 16+n.Size+4+rest
}
func ParseKeyHash(key_hash_string string)(uint64,uint32) {
    key_hash_bytes, khe := hex.DecodeString(key_hash_string)
    key_hash_len := len(key_hash_bytes)
    if khe != nil || key_hash_len <= 4 {
        log.Println("Invalid key_hash", key_hash_string, "length:", key_hash_len, "error", khe)
        return 0, 0
    }
    key := BytesToUint64(key_hash_bytes[0 : key_hash_len-4])
    hash := BytesToUint32(key_hash_bytes[key_hash_len-4 : key_hash_len])
    return key, hash
}
