package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"net/http"
)

func DeleteFile(server string, fileId string) error {
	fileUrl, err := LookupFileId(server, fileId)
	if err != nil {
		return err
	}
	return Delete(fileUrl)
}
func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		glog.V(0).Infoln("failing to delete", url)
		return err
	}
	_, err = client.Do(req)
	return err
}
