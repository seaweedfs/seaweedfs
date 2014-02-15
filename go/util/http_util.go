package util

import (
	"code.google.com/p/weed-fs/go/glog"
	"io/ioutil"
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

func Get(url string) ([]byte, error) {
	r, err := http.Get(url)
	if err != nil {
		glog.V(0).Infoln("getting ", url, err)
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.V(0).Infoln("read get result from", url, err)
		return nil, err
	}
	return b, nil
}
