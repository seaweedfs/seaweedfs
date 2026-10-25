package app

import (
	"fmt"
	"net/url"
)

func fileBrowserPathURL(path string) string {
	return "/files?path=" + url.QueryEscape(path)
}

func fileBrowserPageURL(path, lastFileName string, limit int) string {
	return fmt.Sprintf("%s&lastFileName=%s&limit=%d",
		fileBrowserPathURL(path),
		url.QueryEscape(lastFileName),
		limit,
	)
}
