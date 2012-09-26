package operation

import (
	"bytes"
	"encoding/json"
	_ "fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
)

type UploadResult struct {
	Size int
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
		println("uploading to", uploadUrl)
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
		println("upload response to", uploadUrl, resp_body)
		panic(err.Error())
	}
	//fmt.Println("Uploaded " + strconv.Itoa(ret.Size) + " Bytes to " + uploadUrl)
	return &ret, nil
}
