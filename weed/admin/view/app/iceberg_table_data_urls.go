package app

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
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

// previewIsWorkerSourced reports whether these rows came from a plugin worker
// rather than from metadata admin read itself. Snapshots and data files are
// Iceberg's shape, and showing them empty for another format reads as a fault.
func previewIsWorkerSourced(data dash.IcebergDataPreviewData) bool {
	return data.Format != "" && !strings.EqualFold(data.Format, s3tables.FormatIceberg)
}
