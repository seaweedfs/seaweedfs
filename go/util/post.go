package util

import (
	"io/ioutil"
	"code.google.com/p/weed-fs/go/glog"
	"net/http"
	"net/url"
)

func Post(url string, values url.Values) ([]byte, error) {
	r, err := http.PostForm(url, values)
	if err != nil {
		glog.V(0).Infoln("post to", url, err)
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.V(0).Infoln("read post result from", url, err)
		return nil, err
	}
	return b, nil
}
