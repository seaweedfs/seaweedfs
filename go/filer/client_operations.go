package filer

import ()

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

type ApiRequest struct {
	Command     string //"listFiles", "listDirectories"
	DirectoryId DirectoryId
	FileName    string
}

type ListFilesResult struct {
	Files []FileEntry
	Error string `json:"error,omitempty"`
}

func ListFiles(server string, directoryId DirectoryId, fileName string) (*ListFilesResult, error) {
	var ret ListFilesResult
	if err := call(server, ApiRequest{Command: "listFiles", DirectoryId: directoryId, FileName: fileName}, &ret); err == nil {
		if ret.Error != "" {
			return nil, errors.New(ret.Error)
		}
		return &ret, nil
	} else {
		return nil, err
	}
}

type ListDirectoriesResult struct {
	Directories []DirectoryEntry
	Error       string `json:"error,omitempty"`
}

func ListDirectories(server string, directoryId DirectoryId) (*ListDirectoriesResult, error) {
	var ret ListDirectoriesResult
	if err := call(server, ApiRequest{Command: "listDirectories", DirectoryId: directoryId}, &ret); err == nil {
		if ret.Error != "" {
			return nil, errors.New(ret.Error)
		}
		return &ret, nil
	} else {
		return nil, err
	}
}

func call(server string, request ApiRequest, ret interface{}) error {
	b, err := json.Marshal(request)
	if err != nil {
		fmt.Println("error:", err)
		return nil
	}
	values := make(url.Values)
	values.Add("request", string(b))
	jsonBlob, err := util.Post("http://"+server+"/__api__", values)
	if err != nil {
		return err
	}
	err = json.Unmarshal(jsonBlob, ret)
	if err != nil {
		return err
	}
	return nil
}

