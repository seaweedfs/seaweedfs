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
		wantFaults, wantRefusals := 0, 1
		if c.fault {
			wantFaults, wantRefusals = 1, 0
		}
		if len(faults) != wantFaults || len(refusals) != wantRefusals {
			t.Errorf("%v: %d ERROR and %d refusal log(s), want %d and %d (refusals %v, faults %v)", c.err, len(faults), len(refusals), wantFaults, wantRefusals, refusals, faults)
		}
	}
}
