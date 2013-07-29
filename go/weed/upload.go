package main

import (
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	uploadReplication *string
	uploadDir         *string
	include           *string
)

func init() {
	cmdUpload.Run = runUpload // break init cycle
	cmdUpload.IsDebug = cmdUpload.Flag.Bool("debug", false, "verbose debug information")
	server = cmdUpload.Flag.String("server", "localhost:9333", "weedfs master location")
	uploadDir = cmdUpload.Flag.String("dir", "", "Upload the whole folder recursively if specified.")
	include = cmdUpload.Flag.String("include", "", "pattens of files to upload, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	uploadReplication = cmdUpload.Flag.String("replication", "", "replication type(000,001,010,100,110,200)")
}

var cmdUpload = &Command{
	UsageLine: "upload -server=localhost:9333 file1 [file2 file3]\n upload -server=localhost:9333 -dir=one_directory -include=*.pdf",
	Short:     "upload one or a list of files",
	Long: `upload one or a list of files, or batch upload one whole folder recursively.
	
	If uploading a list of files:
  It uses consecutive file keys for the list of files.
  e.g. If the file1 uses key k, file2 can be read via k_1

  If uploading a whole folder recursively:
  All files under the folder and subfolders will be uploaded, each with its own file key.
  Optional parameter "-include" allows you to specify the file name patterns.
  
  If any file has a ".gz" extension, the content are considered gzipped already, and will be stored as is.
  This can save volume server's gzipped processing and allow customizable gzip compression level.
  The file name will strip out ".gz" and stored. For example, "jquery.js.gz" will be stored as "jquery.js".

  `,
}

type AssignResult struct {
	Fid       string `json:"fid"`
	Url       string `json:"url"`
	PublicUrl string `json:"publicUrl"`
	Count     int
	Error     string `json:"error"`
}

func assign(count int) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.Itoa(count))
	if *uploadReplication != "" {
		values.Add("replication", *uploadReplication)
	}
	jsonBlob, err := util.Post("http://"+*server+"/dir/assign", values)
	debug("assign result :", string(jsonBlob))
	if err != nil {
		return nil, err
	}
	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Count <= 0 {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func upload(filename string, server string, fid string) (int, error) {
	debug("Start uploading file:", filename)
	fh, err := os.Open(filename)
	if err != nil {
		debug("Failed to open file:", filename)
		return 0, err
	}
	fi, fiErr := fh.Stat()
	if fiErr != nil {
		debug("Failed to stat file:", filename)
		return 0, fiErr
	}
	filename = path.Base(filename)
	isGzipped := path.Ext(filename) == ".gz"
	if isGzipped {
		filename = filename[0 : len(filename)-3]
	}
	mtype := mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	ret, e := operation.Upload("http://"+server+"/"+fid+"?ts="+strconv.Itoa(int(fi.ModTime().Unix())), filename, fh, isGzipped, mtype)
	if e != nil {
		return 0, e
	}
	return ret.Size, e
}

type SubmitResult struct {
	FileName string `json:"fileName"`
	FileUrl  string `json:"fileUrl"`
	Fid      string `json:"fid"`
	Size     int    `json:"size"`
	Error    string `json:"error"`
}

func submit(files []string) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file
	}
	ret, err := assign(len(files))
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

func runUpload(cmd *Command, args []string) bool {
	if len(cmdUpload.Flag.Args()) == 0 {
		if *uploadDir == "" {
			return false
		}
		filepath.Walk(*uploadDir, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				if !info.IsDir() {
					if *include != "" {
						if ok, _ := filepath.Match(*include, filepath.Base(path)); !ok {
							return nil
						}
					}
					results, e := submit([]string{path})
					bytes, _ := json.Marshal(results)
					fmt.Println(string(bytes))
					if e != nil {
						return e
					}
				}
			} else {
				fmt.Println(err)
			}
			return err
		})
	} else {
		results, _ := submit(args)
		bytes, _ := json.Marshal(results)
		fmt.Println(string(bytes))
	}
	return true
}
