package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
	"net/http"
)

func DeleteFile(server string, fileId string) (error) {
	fid, parseErr := storage.ParseFileId(fileId)
	if parseErr != nil {
		return parseErr
	}
	lookup, lookupError := Lookup(server,fid.VolumeId)
	if lookupError != nil {
	  return lookupError
	}
	if len(lookup.Locations) == 0 {
	  return nil
	}
	return Delete("http://"+lookup.Locations[0].PublicUrl+"/"+fileId)
}
func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		glog.V(0).Infoln("failing to delete", url)
		return err
	}
	_, err = http.DefaultClient.Do(req)
	return err
}
