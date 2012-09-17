package util

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

func Post(url string, values url.Values) ([]byte, error) {
	r, err := http.PostForm(url, values)
	if err != nil {
		log.Println("post:", err)
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("post:", err)
		return nil, err
	}
	return b, nil
}
