package app

import (
	"net/url"
	"strconv"
)

// icebergTableDataURL builds the table data-preview page URL. Zero snapshotID
// means the current snapshot; zero limit means the server default.
func icebergTableDataURL(catalog, namespace, table string, snapshotID int64, limit int, file string) string {
	u := "/object-store/s3tables/buckets/" + url.PathEscape(catalog) +
		"/namespaces/" + url.PathEscape(namespace) +
		"/tables/" + url.PathEscape(table) + "/data"
	q := url.Values{}
	if snapshotID != 0 {
		q.Set("snapshot", strconv.FormatInt(snapshotID, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if file != "" {
		q.Set("file", file)
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}
