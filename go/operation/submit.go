package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

type SubmitResult struct {
	FileName string `json:"fileName"`
	FileUrl  string `json:"fileUrl"`
	Fid      string `json:"fid"`
	Size     int    `json:"size"`
	Error    string `json:"error"`
}

func Submit(master string, reader io.Reader, replication string) (result SubmitResult, err error) {
	assignResult, assignError := Assign(master, 1, replication)
	if assignError != nil {
		result.Error = assignError.Error()
		return
	}
	url := "http://" + assignResult.PublicUrl + "/" + assignResult.Fid
	uploadResult, uploadError := Upload(url, "", reader, false, "")
	if uploadError != nil {
		result.Error = uploadError.Error()
		return
	}
	result.Size = uploadResult.Size
	result.FileUrl = url
	result.Fid = assignResult.Fid
	return result, nil
}

func SubmitFiles(master string, files []string, replication string) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file
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
		results[index].Size, err = upload(file, ret.PublicUrl, fid)
		if err != nil {
			fid = ""
			results[index].Error = err.Error()
		}
		results[index].Fid = fid
		results[index].FileUrl = ret.PublicUrl + "/" + fid
	}
	return results, nil
}

func upload(filename string, server string, fid string) (int, error) {
	glog.V(2).Info("Start uploading file:", filename)
	fh, err := os.Open(filename)
	if err != nil {
		glog.V(0).Info("Failed to open file: ", filename)
		return 0, err
	}
	fi, fiErr := fh.Stat()
	if fiErr != nil {
		glog.V(0).Info("Failed to stat file:", filename)
		return 0, fiErr
	}
	filename = path.Base(filename)
	isGzipped := path.Ext(filename) == ".gz"
	if isGzipped {
		filename = filename[0 : len(filename)-3]
	}
	mtype := mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	ret, e := Upload("http://"+server+"/"+fid+"?ts="+strconv.Itoa(int(fi.ModTime().Unix())), filename, fh, isGzipped, mtype)
	if e != nil {
		return 0, e
	}
	return ret.Size, e
}
