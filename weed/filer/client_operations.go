package filer

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/chrislusf/seaweedfs/weed/util"
)

type ApiRequest struct {
	Command   string //"listFiles", "listDirectories", "getFileSize"
	Directory string
	FileName  string
	FileId    string
}

type ListFilesResult struct {
	Files []FileEntry
	Error string `json:"error,omitempty"`
}

func ListFiles(server string, directory string, fileName string) (ret *ListFilesResult, err error) {
	ret = new(ListFilesResult)
	if err = call(server, ApiRequest{Command: "listFiles", Directory: directory, FileName: fileName}, ret); err == nil {
		if ret.Error != "" {
			return nil, errors.New(ret.Error)
		}
		return ret, nil
	}
	return nil, err
}

type GetFileSizeResult struct {
	Size  uint64
	Error string `json:"error,omitempty"`
}

func GetFileSize(server string, fileId string) (ret *GetFileSizeResult, err error) {
	ret = new(GetFileSizeResult)
	if err = call(server, ApiRequest{Command: "getFileSize", FileId: fileId}, ret); err == nil {
		if ret.Error != "" {
			return nil, errors.New(ret.Error)
		}
		return ret, nil
	}
	return nil, err
}

type GetFileContentResult struct {
	Content []byte
	Error   string `json:"error,omitempty"`
}

func GetFileContent(server string, fileId string) (ret *GetFileContentResult, err error) {
	ret = new(GetFileContentResult)
	if err = call(server, ApiRequest{Command: "getFileContent", FileId: fileId}, ret); err == nil {
		if ret.Error != "" {
			return nil, errors.New(ret.Error)
		}
		return ret, nil
	}
	return nil, err
}

type ListDirectoriesResult struct {
	Directories []DirectoryEntry
	Error       string `json:"error,omitempty"`
}

func ListDirectories(server string, directory string) (ret *ListDirectoriesResult, err error) {
	ret = new(ListDirectoriesResult)
	if err := call(server, ApiRequest{Command: "listDirectories", Directory: directory}, ret); err == nil {
		if ret.Error != "" {
			return nil, errors.New(ret.Error)
		}
		return ret, nil
	}
	return nil, err
}

func DeleteDirectoryOrFile(server string, path string, isDir bool) error {
	destUrl := fmt.Sprintf("http://%s%s", server, path)
	if isDir {
		destUrl += "/?recursive=true"
	}
	return util.Delete(destUrl, "")
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
