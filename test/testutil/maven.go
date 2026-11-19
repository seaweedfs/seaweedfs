package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var mavenCentralMirrors = []string{
	"https://repo1.maven.org/maven2",
	"https://maven-central.storage-download.googleapis.com/maven2",
}

var (
	mavenDownloadBudget = 3 * time.Minute
	mavenAttemptTimeout = 75 * time.Second
)

// MavenJar returns a local copy of a Maven Central jar, verified against its
// pinned SHA-256. The jar is kept under the user cache directory so every test
// in a run, and every run that restores that directory, shares one download.
func MavenJar(t testing.TB, group, artifact, version, sha256Hex string) string {
	t.Helper()

	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		cacheRoot = os.TempDir()
	}
	dir := filepath.Join(cacheRoot, "seaweedfs-test-jars")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create the jar cache directory: %v", err)
	}
	name := fmt.Sprintf("%s-%s.jar", artifact, version)
	path := filepath.Join(dir, name)
	if fileSHA256(path) == sha256Hex {
		return path
	}

	ctx, cancel := context.WithTimeout(context.Background(), mavenDownloadBudget)
	defer cancel()
	urlPath := fmt.Sprintf("%s/%s/%s/%s", strings.ReplaceAll(group, ".", "/"), artifact, version, name)
	var lastErr error
	for attempt := 0; attempt < 3 && ctx.Err() == nil; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(time.Duration(attempt) * 10 * time.Second):
			}
		}
		for _, mirror := range mavenCentralMirrors {
			if ctx.Err() != nil {
				break
			}
			attemptCtx, cancelAttempt := context.WithTimeout(ctx, mavenAttemptTimeout)
			lastErr = downloadVerified(attemptCtx, mirror+"/"+urlPath, path, sha256Hex)
			cancelAttempt()
			if lastErr == nil {
				return path
			}
			t.Logf("download %s: %v", name, lastErr)
		}
	}
	t.Fatalf("download %s from Maven Central: %v", name, lastErr)
	return ""
}

func fileSHA256(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

func downloadVerified(ctx context.Context, url, path, sha256Hex string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(tmp, h), resp.Body)
	closeErr := tmp.Close()
	if copyErr != nil {
		return fmt.Errorf("GET %s: %w", url, copyErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sha256Hex {
		return fmt.Errorf("GET %s: sha256 %s, want %s", url, got, sha256Hex)
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
