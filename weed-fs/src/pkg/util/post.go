package util

import (
    "http"
    "io/ioutil"
    "url"
    "log"
)

func Post(url string, values url.Values)[]byte{
    r, err := http.PostForm(url, values)
    if err != nil {
        log.Println("post:", err)
        return nil
    }
    defer r.Body.Close()
    b, err := ioutil.ReadAll(r.Body)
    if err != nil {
        log.Println("post:", err)
        return nil
    }
    return b
}
