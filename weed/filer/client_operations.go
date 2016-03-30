package filer

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/util"

	"net/url"
)

type ApiRequest struct {
	Command   string //"listFiles", "listDirectories"
	Directory string
	FileName  string
}

type ListFilesResult struct {
	Files []FileEntry
	Error string `json:"error,omitempty"`
}

func ListFiles(server string, directory string, fileName string) (*ListFilesResult, error) {
	var ret ListFilesResult
	if err := call(server, ApiRequest{Command: "listFiles", Directory: directory, FileName: fileName}, &ret); err == nil {
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

func ListDirectories(server string, directory string) (*ListDirectoriesResult, error) {
	var ret ListDirectoriesResult
	if err := call(server, ApiRequest{Command: "listDirectories", Directory: directory}, &ret); err == nil {
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
	jsonBlob, err := util.Post(server, "/__api__", values)
	if err != nil {
		return err
	}
	err = json.Unmarshal(jsonBlob, ret)
	if err != nil {
		return err
	}
	return nil
}
