package sse_test

import (
    "fmt"
    "os"
    "testing"

    "github.com/seaweedfs/seaweedfs/test/s3/testutil"
)

var testServer *testutil.TestServer

// TestMain sets up and tears down the test environment using weed mini
func TestMain(m *testing.M) {
    var exitCode int

    fmt.Println("TestMain: Starting test setup...")

    // Check if we should manage the server or use an existing one
    if os.Getenv("USE_EXTERNAL_SERVER") != "true" {
        // Start server using weed mini
        fmt.Println("TestMain: Starting weed mini server...")
        config := testutil.DefaultServerConfig(nil)
        config.AccessKey = "some_access_key1"
        config.SecretKey = "some_secret_key1"

        var err error
        testServer, err = testutil.StartServer(config)
        if err != nil {
            fmt.Printf("TestMain: ERROR - Failed to start test server: %v\n", err)
            os.Exit(1)
        }
        fmt.Println("TestMain: Server started successfully")
        defer testServer.Stop()
    }

    // Run tests
    fmt.Println("TestMain: Running tests...")
    exitCode = m.Run()
    fmt.Printf("TestMain: Tests completed with exit code %d\n", exitCode)
    os.Exit(exitCode)
}
