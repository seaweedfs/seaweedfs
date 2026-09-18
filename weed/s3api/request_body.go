package s3api

import (
	"errors"
	"io"
	"net/http"
)

var errRequestBodyTooLarge = errors.New("request body too large")

func readRequestBody(r *http.Request, limit int64) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errRequestBodyTooLarge
	}
	return body, nil
}
