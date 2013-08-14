package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"io"
	"mime"
	"os"
	"path"
	"strconv"
	"strings"
)

type FilePart struct {
	ReadCloser io.ReadCloser //required, all rest are optional
	FileName   string
	IsGzipped  bool
	MimeType   string
	ModTime    int64 //in seconds
}

type SubmitResult struct {
	FileName string `json:"fileName"`
	FileUrl  string `json:"fileUrl"`
	Fid      string `json:"fid"`
	Size     int    `json:"size"`
	Error    string `json:"error"`
}

func SubmitFiles(master string, files []FilePart, replication string) ([]SubmitResult, error) {
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
		results[index].Size, err = file.Upload(ret.PublicUrl, fid)
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
		if ret[index], err = NewFilePart(file); err != nil {
			return
		}
	}
	return
}
func NewFilePart(fullPathFilename string) (ret FilePart, err error) {
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		glog.V(0).Info("Failed to open file: ", fullPathFilename)
		return ret, openErr
	}
	ret.ReadCloser = fh

	if fi, fiErr := fh.Stat(); fiErr != nil {
		glog.V(0).Info("Failed to stat file:", fullPathFilename)
		return ret, fiErr
	} else {
		ret.ModTime = fi.ModTime().UTC().Unix()
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

func (fi FilePart) Upload(server string, fid string) (int, error) {
	fileUrl := "http://" + server + "/" + fid
	if fi.ModTime != 0 {
		fileUrl += "?ts=" + strconv.Itoa(int(fi.ModTime))
	}
	defer fi.ReadCloser.Close()
	ret, e := Upload(fileUrl, fi.FileName, fi.ReadCloser, fi.IsGzipped, fi.MimeType)
	if e != nil {
		return 0, e
	}
	return ret.Size, e
}
