package filer

import ()

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	_ "fmt"
	"net/url"
	"strconv"
)

type ListFilesResult struct {
	Files []FileEntry
	Error string `json:"error,omitempty"`
}

func ListFiles(server string, directoryId DirectoryId, fileName string) (*ListFilesResult, error) {
	values := make(url.Values)
	values.Add("directoryId", strconv.Itoa(int(directoryId)))
	jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
	if err != nil {
		return nil, err
	}
	var ret ListFilesResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

type ListDirectoriesResult struct {
	Directories []DirectoryEntry
	Error       string `json:"error,omitempty"`
}

func ListDirectories(server string, directoryId DirectoryId) (*ListDirectoriesResult, error) {
	values := make(url.Values)
	values.Add("directoryId", strconv.Itoa(int(directoryId)))
	jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
	if err != nil {
		return nil, err
	}
	var ret ListDirectoriesResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
