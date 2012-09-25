package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
  "pkg/operation"
	"pkg/util"
	"strconv"
)

var uploadReplication *string

func init() {
	cmdUpload.Run = runUpload // break init cycle
	IsDebug = cmdUpload.Flag.Bool("debug", false, "verbose debug information")
	server = cmdUpload.Flag.String("server", "localhost:9333", "weedfs master location")
	uploadReplication = cmdUpload.Flag.String("replication", "00", "replication type(00,01,10,11)")
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
	if *IsDebug {
		fmt.Println("debug", *IsDebug, "assign result :", string(jsonBlob))
	}
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

func upload(filename string, server string, fid string) (int) {
	if *IsDebug {
		fmt.Println("Start uploading file:", filename)
	}
	fh, err := os.Open(filename)
	if err != nil {
		if *IsDebug {
			fmt.Println("Failed to open file:", filename)
		}
		panic(err.Error())
	}
	ret, _ := operation.Upload("http://"+server+"/"+fid, filename, fh)
	return ret.Size
}

type SubmitResult struct {
	Fid  string "fid"
	Size int    "size"
}

func submit(files []string) []SubmitResult {
	ret, err := assign(len(files))
	if err != nil {
		panic(err)
	}
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		fid := ret.Fid
		if index > 0 {
			fid = fid + "_" + strconv.Itoa(index)
		}
		results[index].Size = upload(file, ret.PublicUrl, fid)
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
