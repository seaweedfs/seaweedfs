package operation

import (
	"bytes"
	"io"
	"mime"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/security"
)

type FilePart struct {
	Reader      io.Reader
	FileName    string
	FileSize    int64
	IsGzipped   bool
	MimeType    string
	ModTime     int64 //in seconds
	Replication string
	Collection  string
	Ttl         string
	Server      string //this comes from assign result
	Fid         string //this comes from assign result, but customizable
}

type SubmitResult struct {
	FileName string `json:"fileName,omitempty"`
	FileUrl  string `json:"fileUrl,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Size     uint32 `json:"size,omitempty"`
	Error    string `json:"error,omitempty"`
}

func SubmitFiles(master string, files []FilePart,
	replication string, collection string, ttl string, maxMB int,
	secret security.Secret,
) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
	}
	ret, err := Assign(master, len(files), replication, collection, ttl)
	if err != nil {
		for index, _ := range files {
			results[index].Error = err.Error()
		}
		return results, err
	}
	for index, file := range files {
		file.Fid = ret.Fid
		if index > 0 {
			file.Fid = file.Fid + "_" + strconv.Itoa(index)
		}
		file.Server = ret.Url
		file.Replication = replication
		file.Collection = collection
		results[index].Size, err = file.Upload(maxMB, master, secret)
		if err != nil {
			results[index].Error = err.Error()
		}
		results[index].Fid = file.Fid
		results[index].FileUrl = file.Server + "/" + file.Fid
	}
	return results, nil
}

func NewFileParts(fullPathFilenames []string) (ret []FilePart, err error) {
	ret = make([]FilePart, len(fullPathFilenames))
	for index, file := range fullPathFilenames {
		if ret[index], err = newFilePart(file); err != nil {
			return
		}
	}
	return
}
func newFilePart(fullPathFilename string) (ret FilePart, err error) {
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		glog.V(0).Info("Failed to open file: ", fullPathFilename)
		return ret, openErr
	}
	ret.Reader = fh

	if fi, fiErr := fh.Stat(); fiErr != nil {
		glog.V(0).Info("Failed to stat file:", fullPathFilename)
		return ret, fiErr
	} else {
		ret.ModTime = fi.ModTime().UTC().Unix()
		ret.FileSize = fi.Size()
	}
	ext := strings.ToLower(path.Ext(fullPathFilename))
	ret.IsGzipped = ext == ".gz"
	if ret.IsGzipped {
		ret.FileName = fullPathFilename[0 : len(fullPathFilename)-3]
	}
	ret.FileName = fullPathFilename
	if ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return ret, nil
}

func (fi FilePart) Upload(maxMB int, master string, secret security.Secret) (retSize uint32, err error) {
	jwt := security.GenJwt(secret, fi.Fid)
	fileUrl := "http://" + fi.Server + "/" + fi.Fid
	if fi.ModTime != 0 {
		fileUrl += "?ts=" + strconv.Itoa(int(fi.ModTime))
	}
	if closer, ok := fi.Reader.(io.Closer); ok {
		defer closer.Close()
	}
	if maxMB > 0 && fi.FileSize > int64(maxMB*1024*1024) {
		chunkSize := int64(maxMB * 1024 * 1024)
		chunks := fi.FileSize/chunkSize + 1
		fids := make([]string, 0)
		for i := int64(0); i < chunks; i++ {
			id, count, e := upload_one_chunk(
				fi.FileName+"-"+strconv.FormatInt(i+1, 10),
				io.LimitReader(fi.Reader, chunkSize),
				master, fi.Replication, fi.Collection, fi.Ttl,
				jwt)
			if e != nil {
				return 0, e
			}
			fids = append(fids, id)
			retSize += count
		}
		err = upload_file_id_list(fileUrl, fi.FileName+"-list", fids, jwt)
	} else {
		ret, e := Upload(fileUrl, fi.FileName, fi.Reader, fi.IsGzipped, fi.MimeType, jwt)
		if e != nil {
			return 0, e
		}
		return ret.Size, e
	}
	return
}

func upload_one_chunk(filename string, reader io.Reader, master,
	replication string, collection string, ttl string, jwt security.EncodedJwt,
) (fid string, size uint32, e error) {
	ret, err := Assign(master, 1, replication, collection, ttl)
	if err != nil {
		return "", 0, err
	}
	fileUrl, fid := "http://"+ret.Url+"/"+ret.Fid, ret.Fid
	glog.V(4).Info("Uploading part ", filename, " to ", fileUrl, "...")
	uploadResult, uploadError := Upload(fileUrl, filename, reader, false,
		"application/octet-stream", jwt)
	if uploadError != nil {
		return fid, 0, uploadError
	}
	return fid, uploadResult.Size, nil
}

func upload_file_id_list(fileUrl, filename string, fids []string, jwt security.EncodedJwt) error {
	var buf bytes.Buffer
	buf.WriteString(strings.Join(fids, "\n"))
	glog.V(4).Info("Uploading final list ", filename, " to ", fileUrl, "...")
	_, e := Upload(fileUrl, filename, &buf, false, "text/plain", jwt)
	return e
}
