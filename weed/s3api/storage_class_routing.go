package s3api

import (
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const defaultStorageClass = "STANDARD"

var storageClassDefaults = map[string]string{
	"STANDARD":            "ssd",
	"REDUCED_REDUNDANCY":  "hdd",
	"STANDARD_IA":         "hdd",
	"ONEZONE_IA":          "hdd",
	"INTELLIGENT_TIERING": "hdd",
	"GLACIER":             "hdd",
	"DEEP_ARCHIVE":        "hdd",
	"OUTPOSTS":            "hdd",
	"GLACIER_IR":          "hdd",
	"SNOW":                "hdd",
}

func normalizeStorageClass(storageClass string) string {
	return strings.ToUpper(strings.TrimSpace(storageClass))
}

func loadStorageClassDiskTypeMap(overrides map[string]string) map[string]string {
	mappings := make(map[string]string)
	normalizedOverrides := make(map[string]string)
	for k, v := range overrides {
		normalizedOverrides[normalizeStorageClass(k)] = v
	}
	for storageClass, defaultDiskType := range storageClassDefaults {
		diskType := defaultDiskType
		if v, ok := normalizedOverrides[storageClass]; ok {
			diskType = strings.TrimSpace(v)
		}
		if diskType == "" {
			continue
		}
		mappings[storageClass] = diskType
	}
	return mappings
}

func resolveEffectiveStorageClass(header http.Header, entryExtended map[string][]byte) (string, s3err.ErrorCode) {
	if header != nil {
		if fromHeader := strings.TrimSpace(header.Get(s3_constants.AmzStorageClass)); fromHeader != "" {
			storageClass := normalizeStorageClass(fromHeader)
			if !validateStorageClass(storageClass) {
				return "", s3err.ErrInvalidStorageClass
			}
			return storageClass, s3err.ErrNone
		}
	}

	if entryExtended != nil {
		if fromEntry := strings.TrimSpace(string(entryExtended[s3_constants.AmzStorageClass])); fromEntry != "" {
			storageClass := normalizeStorageClass(fromEntry)
			if validateStorageClass(storageClass) {
				return storageClass, s3err.ErrNone
			}
		}
	}

	return defaultStorageClass, s3err.ErrNone
}

func (s3a *S3ApiServer) mapStorageClassToDiskType(storageClass string) string {
	if len(s3a.storageClassDiskTypes) == 0 {
		return ""
	}
	return s3a.storageClassDiskTypes[normalizeStorageClass(storageClass)]
}
