package filer

import (
	"fmt"
	"net/url"

	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strconv"
)

type SubmitResult struct {
	FileName string `json:"fileName,omitempty"`
	FileUrl  string `json:"fileUrl,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Size     uint32 `json:"size,omitempty"`
	Error    string `json:"error,omitempty"`
}

func RegisterFile(filer string, path string, fileId string, fileSize int64, secret security.Secret) error {
	// TODO: jwt need to be used
	_ = security.GenJwt(secret, fileId)

	values := make(url.Values)
	values.Add("path", path)
	values.Add("fileId", fileId)
	values.Add("fileSize", strconv.FormatInt(fileSize, 10))
	_, err := util.Post("http://"+filer+"/admin/register", values)
	if err != nil {
		return fmt.Errorf("Failed to register path %s on filer %s to file id %s : %v", path, filer, fileId, err)
	}
	return nil
}
