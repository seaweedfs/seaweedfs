package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/util"
	"strconv"
)

var uploadReplication *string

func init() {
	cmdUpload.Run = runUpload // break init cycle
	cmdUpload.IsDebug = cmdUpload.Flag.Bool("debug", false, "verbose debug information")
	server = cmdUpload.Flag.String("server", "localhost:9333", "weedfs master location")
	uploadReplication = cmdUpload.Flag.String("replication", "000", "replication type(000,001,010,100,110,200)")
}

var cmdUpload = &Command{
	UsageLine: "upload -server=localhost:9333 file1 [file2 file3]",
	Short:     "upload one or a list of files",
	Long: `upload one or a list of files. 
  It uses consecutive file keys for the list of files.
  e.g. If the file1 uses key k, file2 can be read via k_1

  `,
}

type AssignResult struct {
	Fid       string "fid"
	Url       string "url"
	PublicUrl string "publicUrl"
	Count     int
	Error     string "error"
}

func assign(count int) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.Itoa(count))
	values.Add("replication", *uploadReplication)
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
	ret, e := operation.Upload("http://"+server+"/"+fid, path.Base(filename), fh)
	if e != nil {
		return 0, e
	}
	return ret.Size, e
}

type SubmitResult struct {
	Fid   string "fid"
	Size  int    "size"
	Error string "error"
}

func submit(files []string) []SubmitResult {
	ret, err := assign(len(files))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	results := make([]SubmitResult, len(files))
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
	}
	return results
}

func runUpload(cmd *Command, args []string) bool {
	*IsDebug = true
	if len(cmdUpload.Flag.Args()) == 0 {
		return false
	}
	results := submit(args)
	bytes, _ := json.Marshal(results)
	fmt.Print(string(bytes))
	return true
}
