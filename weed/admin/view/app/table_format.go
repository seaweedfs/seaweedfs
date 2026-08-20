package app

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// Shared rendering for the table format a bucket or table carries. A bucket is a
// catalog and a catalog serves one protocol, so the format decides which
// endpoint a client uses and which panels on a page mean anything.

// formatLabel is what the badge says. A bucket created before formats were
// declared has none; that is a fact about its age, not a fault, so it says so
// plainly rather than claiming a format it never chose.
func formatLabel(format string) string {
	if strings.TrimSpace(format) == "" {
		return "unset"
	}
	return strings.ToUpper(format)
}

// formatBadgeClass keeps the two formats distinguishable at a glance without
// reaching for a colour that means something else in this UI.
func formatBadgeClass(format string) string {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case s3tables.FormatIceberg:
		return "badge bg-primary"
	case s3tables.FormatLance:
		return "badge bg-success"
	default:
		return "badge bg-light text-muted border"
	}
}

// formatBadgeTitle explains the badge on hover, which is the only place there is
// room to say what an undeclared bucket does.
func formatBadgeTitle(format string) string {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case s3tables.FormatIceberg:
		return "Served over the Iceberg REST catalog"
	case s3tables.FormatLance:
		return "Served over the Lance Namespace API"
	default:
		return "Created before formats were declared; accepts tables of either format"
	}
}

// isLanceFormat reports whether this is a format the cluster records but cannot
// read, which is what decides between the Iceberg panels and the worker's.
func isLanceFormat(format string) bool {
	return strings.EqualFold(strings.TrimSpace(format), s3tables.FormatLance)
}

// bucketCatalogPath is the path a client uses to reach one bucket, which differs
// per format because the two are different protocols on different ports.
func bucketCatalogPath(format, bucket string) string {
	if isLanceFormat(format) {
		return fmt.Sprintf("/v1/namespace/%s/list", bucket)
	}
	return fmt.Sprintf("/v1/%s/namespaces", bucket)
}
