package operation

import (
	"bytes"
	"encoding/json"
	"errors"
	_ "fmt"
	"io"
	"io/ioutil"
	"log"
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
	io.Copy(file_writer, reader)
	content_type := body_writer.FormDataContentType()
	body_writer.Close()
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
