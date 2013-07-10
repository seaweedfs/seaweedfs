package operation

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	_ "fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
)

type UploadResult struct {
	Size  int
	Error string
}

func Upload(uploadUrl string, filename string, reader io.Reader) (*UploadResult, error) {
	body_buf := bytes.NewBufferString("")
	body_writer := multipart.NewWriter(body_buf)
	file_writer, err := body_writer.CreateFormFile("file", filename)
	if err != nil {
		log.Println("error creating form file", err)
		return nil, err
	}
	if _, err = io.Copy(file_writer, reader); err != nil {
		log.Println("error copying data", err)
		return nil, err
	}
	content_type := mime.TypeByExtension(filepath.Ext(filename))
  content_type := body_writer.FormDataContentType()
	if err = body_writer.Close(); err != nil {
		log.Println("error closing body", err)
		return nil, err
	}
	resp, err := http.Post(uploadUrl, content_type, body_buf)
	if err != nil {
		log.Println("failing to upload to", uploadUrl)
		return nil, err
	}
	defer resp.Body.Close()
	resp_body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var ret UploadResult
	err = json.Unmarshal(resp_body, &ret)
	if err != nil {
		log.Println("failing to read upload resonse", uploadUrl, resp_body)
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
