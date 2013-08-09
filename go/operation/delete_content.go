package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"net/http"
)

func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		glog.V(0).Infoln("failing to delete", url)
		return err
	}
	_, err = http.DefaultClient.Do(req)
	return err
}
