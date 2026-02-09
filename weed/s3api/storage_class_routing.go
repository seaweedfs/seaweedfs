package s3api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const defaultStorageClass = "STANDARD"

func normalizeStorageClass(storageClass string) string {
	return strings.ToUpper(strings.TrimSpace(storageClass))
}

func parseStorageClassDiskTypeMap(raw string) (map[string]string, error) {
	mappings := make(map[string]string)
	if strings.TrimSpace(raw) == "" {
		return mappings, nil
	}

	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}

		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid mapping %q, expected STORAGE_CLASS=diskType", token)
		}

		storageClass := normalizeStorageClass(parts[0])
		if !validateStorageClass(storageClass) {
			return nil, fmt.Errorf("invalid storage class %q in mapping %q", storageClass, token)
		}

		diskType := strings.TrimSpace(parts[1])
		if diskType == "" {
			return nil, fmt.Errorf("empty disk type in mapping %q", token)
		}

		mappings[storageClass] = diskType
	}

	return mappings, nil
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
