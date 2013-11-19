package operation

import (
	"bytes"
	"code.google.com/p/weed-fs/go/glog"
	"io"
	"mime"
	"os"
	"path"
	"strconv"
	"strings"
)

type FilePart struct {
	Reader    io.Reader
	FileName  string
	FileSize  int64
	IsGzipped bool
	MimeType  string
	ModTime   int64 //in seconds
}

type SubmitResult struct {
	FileName string `json:"fileName"`
	FileUrl  string `json:"fileUrl"`
	Fid      string `json:"fid"`
	Size     int    `json:"size"`
	Error    string `json:"error"`
}

func SubmitFiles(master string, files []FilePart, replication string, maxMB int) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
	}
	ret, err := Assign(master, len(files), replication)
	if err != nil {
		for index, _ := range files {
			results[index].Error = err.Error()
		}
		return results, err
	}
	for index, file := range files {
		fid := ret.Fid
		if index > 0 {
			fid = fid + "_" + strconv.Itoa(index)
		}
		results[index].Size, err = file.upload(ret.PublicUrl, fid, maxMB, master, replication)
		if err != nil {
			fid = ""
			results[index].Error = err.Error()
		}
		results[index].Fid = fid
		results[index].FileUrl = ret.PublicUrl + "/" + fid
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

func (fi FilePart) upload(server string, fid string, maxMB int, master, replication string) (retSize int, err error) {
	fileUrl := "http://" + server + "/" + fid
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
			id, count, e := upload_one_chunk(fi.FileName+"-"+strconv.FormatInt(i+1, 10), io.LimitReader(fi.Reader, chunkSize), master, replication)
			if e != nil {
				return 0, e
			}
			fids = append(fids, id)
			retSize += count
		}
		err = upload_file_id_list(fileUrl, fi.FileName+"-list", fids)
	} else {
		ret, e := Upload(fileUrl, fi.FileName, fi.Reader, fi.IsGzipped, fi.MimeType)
		if e != nil {
			return 0, e
		}
		return ret.Size, e
	}
	return
}

func upload_one_chunk(filename string, reader io.Reader, master, replication string) (fid string, size int, e error) {
	ret, err := Assign(master, 1, replication)
	if err != nil {
		return "", 0, err
	}
	fileUrl, fid := "http://"+ret.PublicUrl+"/"+ret.Fid, ret.Fid
	glog.V(4).Info("Uploading part ", filename, " to ", fileUrl, "...")
	uploadResult, uploadError := Upload(fileUrl, filename, reader, false, "application/octet-stream")
	return fid, uploadResult.Size, uploadError
}

func upload_file_id_list(fileUrl, filename string, fids []string) error {
	var buf bytes.Buffer
	buf.WriteString(strings.Join(fids, "\n"))
	glog.V(4).Info("Uploading final list ", filename, " to ", fileUrl, "...")
	_, e := Upload(fileUrl, filename, &buf, false, "text/plain")
	return e
}
