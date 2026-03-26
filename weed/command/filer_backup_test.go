package command

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestMain(m *testing.M) {
	util_http.InitGlobalHttpClient()
	os.Exit(m.Run())
}

// readUrlError starts a test HTTP server returning the given status code
// and returns the error produced by ReadUrlAsStream.
//
// The error format is defined in ReadUrlAsStream:
// https://github.com/seaweedfs/seaweedfs/blob/3a765df2ff90839acb9acf910b73513417fa84d1/weed/util/http/http_global_client_util.go#L353
func readUrlError(t *testing.T, statusCode int) error {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(statusCode), statusCode)
	}))
	defer server.Close()

	_, err := util_http.ReadUrlAsStream(context.Background(),
		server.URL+"/437,03f591a3a2b95e?readDeleted=true", "",
		nil, false, true, 0, 1024, func(data []byte) {})
	if err == nil {
		t.Fatal("expected error from ReadUrlAsStream, got nil")
	}
	return err
}

func TestIsIgnorable404_WrappedErrNotFound(t *testing.T) {
	readErr := readUrlError(t, http.StatusNotFound)
	// genProcessFunction wraps sink errors with %w:
	// https://github.com/seaweedfs/seaweedfs/blob/3a765df2ff90839acb9acf910b73513417fa84d1/weed/command/filer_sync.go#L496
	genErr := fmt.Errorf("create entry1 : %w", readErr)

	if !isIgnorable404(genErr) {
		t.Errorf("expected ignorable, got not: %v", genErr)
	}
}

func TestIsIgnorable404_BrokenUnwrapChain(t *testing.T) {
	readErr := readUrlError(t, http.StatusNotFound)
	// AWS SDK v1 wraps transport errors via awserr.New which uses origErr.Error()
	// instead of %w, so errors.Is cannot unwrap through it:
	// https://github.com/aws/aws-sdk-go/blob/v1.55.8/aws/corehandlers/handlers.go#L173
	// https://github.com/aws/aws-sdk-go/blob/v1.55.8/aws/awserr/types.go#L15
	awsSdkErr := fmt.Errorf("RequestError: send request failed\n"+
		"caused by: Put \"https://s3.amazonaws.com/bucket/key\": %s", readErr.Error())
	genErr := fmt.Errorf("create entry1 : %w", awsSdkErr)

	if !isIgnorable404(genErr) {
		t.Errorf("expected ignorable, got not: %v", genErr)
	}
}

func TestIsIgnorable404_NonIgnorableError(t *testing.T) {
	readErr := readUrlError(t, http.StatusForbidden)
	genErr := fmt.Errorf("create entry1 : %w", readErr)

	if isIgnorable404(genErr) {
		t.Errorf("expected not ignorable, got ignorable: %v", genErr)
	}
}
