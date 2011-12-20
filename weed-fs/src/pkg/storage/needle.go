package storage

import (
	"io"
	"io/ioutil"
	"http"
	"log"
	"strconv"
	"strings"
)

type Needle struct {
	Cookie       uint32  "random number to mitigate brute force lookups"
	Key          uint64 "file id"
	Size         uint32 "Data size"
	Data         []byte "The actual file data"
	Checksum     int32  "CRC32 to check integrity"
	Padding      []byte "Aligned to 8 bytes"
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

	n.ParsePath(r.URL.Path[1:strings.LastIndex(r.URL.Path, ".")])

	return
}
func (n *Needle) ParsePath(path string) {
	a := strings.Split(path, "_")
	log.Println("cookie", a[0], "key", a[1], "altKey", a[2])
	cookie, _ := strconv.Atoui(a[0])
	n.Cookie = uint32(cookie)
	n.Key, _ = strconv.Atoui64(a[1])
}
func (n *Needle) Append(w io.Writer) {
	header := make([]byte, 16)
    uint32toBytes(header[0:4], n.Cookie)
	uint64toBytes(header[4:12], n.Key)
	n.Size = uint32(len(n.Data))
	uint32toBytes(header[12:16], n.Size)
	w.Write(header)
	w.Write(n.Data)
	rest := 8 - ((n.Size + 16 + 4) % 8)
	uint32toBytes(header[0:4], uint32(n.Checksum))
	w.Write(header[0 : rest+4])
}
func (n *Needle) Read(r io.Reader, size uint32) {
	bytes := make([]byte, size+16+4)
	r.Read(bytes)
	n.Cookie = bytesToUint32(bytes[0:4])
	n.Key = bytesToUint64(bytes[4:12])
	n.Size = bytesToUint32(bytes[12:16])
	n.Data = bytes[16 : 16+size]
	n.Checksum = int32(bytesToUint32(bytes[16+size : 16+size+4]))
}
