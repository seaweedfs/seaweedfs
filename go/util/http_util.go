package util

import (
	"code.google.com/p/weed-fs/go/glog"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

var (
	client    *http.Client
	Transport *http.Transport
)

func init() {
	Transport = &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{Transport: Transport}
}

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
	if err != nil {
		glog.V(0).Infoln(err)
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
	r, err := client.Get(url)
	if err != nil {
		glog.V(0).Infoln(err)
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		glog.V(0).Infoln("read get result from", url, err)
		return nil, err
	}
	return b, nil
}

func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		glog.V(0).Infoln("failing to delete", url)
		return err
	}
	resp, e := client.Do(req)
	if e != nil {
		glog.V(0).Infoln(e)
		return e
	}
	defer resp.Body.Close()
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		glog.V(0).Infoln("read get result from", url, err)
		return err
	}
	return nil
}
