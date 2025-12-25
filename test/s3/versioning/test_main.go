package s3api

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/s3/testutil"
)

var testServer *testutil.TestServer

// TestMain sets up and tears down the test environment using weed mini
func TestMain(m *testing.M) {
	var exitCode int

	// Check if we should manage the server or use an existing one
	if os.Getenv("USE_EXTERNAL_SERVER") != "true" {
		// Start server using weed mini
		config := testutil.DefaultServerConfig(nil)
		config.AccessKey = "some_access_key1"
		config.SecretKey = "some_secret_key1"

		var err error
		testServer, err = testutil.StartServer(&testMainHelper{}, config)
		if err != nil {
			panic("Failed to start test server: " + err.Error())
		}
		defer testServer.Stop()
	}

	// Run tests
	exitCode = m.Run()
	os.Exit(exitCode)
}

// testMainHelper implements the interface expected by StartServer
type testMainHelper struct{}

func (h *testMainHelper) Fatalf(format string, args ...interface{}) {
	panic(format)
}

func (h *testMainHelper) Logf(format string, args ...interface{}) {
	// Silent logging in TestMain
}
