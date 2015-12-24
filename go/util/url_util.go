package util

import "net/url"

func MkUrl(host, path string, args url.Values) string {
	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   path,
	}
	u.RawQuery = args.Encode()
	return u.String()
}
