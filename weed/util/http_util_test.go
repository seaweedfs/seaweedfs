package util

import (
	"testing"

	"github.com/valyala/fasthttp"
)

func TestFasthttpClientHead(t *testing.T) {
	err := Head("https://www.google.com", func(header *fasthttp.ResponseHeader) {
		header.VisitAll(func(key, value []byte) {
			println(string(key) + ": " + string(value))
		})
	})
	if err != nil {
		println(err.Error())
	}

}
