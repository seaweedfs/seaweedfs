package iceberg

import (
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestLogManagerErrorRefusalsAreNotErrors(t *testing.T) {
	var refusals, faults []string
	oldR, oldF := logManagerRefusal, logManagerFault
	logManagerRefusal = func(f string, a ...interface{}) { refusals = append(refusals, fmt.Sprintf(f, a...)) }
	logManagerFault = func(f string, a ...interface{}) { faults = append(faults, fmt.Sprintf(f, a...)) }
	defer func() { logManagerRefusal, logManagerFault = oldR, oldF }()
	for _, c := range []struct {
		err   error
		fault bool
	}{
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeAccessDenied, Message: "not authorized to create namespace in this bucket"}, false},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeNoSuchNamespace, Message: "no such namespace"}, false},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeNamespaceAlreadyExists, Message: "exists"}, false},
		{errors.New("filer: connection refused"), true}, // positive control: a real fault stays at ERROR
	} {
		refusals, faults = nil, nil
		logManagerError("CreateNamespace", c.err)
		if got := len(faults) == 1 && len(refusals) == 0; got != c.fault {
			t.Errorf("%v: ERROR=%v want %v (refusals %v, faults %v)", c.err, got, c.fault, refusals, faults)
		}
	}
}
