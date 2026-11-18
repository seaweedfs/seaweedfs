//go:build linux || darwin

package fuse_dlm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestStageSameFilesWithoutDLM has two mounts without -dlm stage the same
// files into one directory at the same time, the way two jobs extract the same
// archive into a shared directory: a file that exists is skipped, otherwise it
// is created, written and closed, then chmod'ed and given its mtime. Both
// mounts often create the same file, and each then changes the attributes of
// the copy it has cached.
//
// Whatever either mount is told, no entry may point at a chunk that is gone
// from the volume server, and every file must read back whole. An update sent
// from a cached copy after the other mount rewrote the file puts back the
// chunks the rewrite dropped, which the filer deletes; reads of such a file
// fail once the volume is vacuumed.
func TestStageSameFilesWithoutDLM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping FUSE integration test in short mode")
	}

	cluster := startTestCluster(t, false)
	t.Cleanup(cluster.Stop)

	const (
		files = 400
		dir   = "staged"
	)
	rng := rand.New(rand.NewSource(1))
	names := make([]string, files)
	content := make(map[string][]byte, files)
	for i := range names {
		names[i] = fmt.Sprintf("img-%04d.jpg", i)
		data := make([]byte, 1024+rng.Intn(63*1024))
		rng.Read(data)
		content[names[i]] = data
	}
	mtime := time.Date(2026, 10, 26, 4, 0, 0, 0, time.UTC)

	// What the losing mount is told depends on the version (nothing, EIO,
	// EAGAIN); the files are what this test checks.
	failed := make([][]string, 2)
	setupErr := make([]error, 2)
	wrote := [2][]bool{make([]bool, files), make([]bool, files)}
	var ready, wg sync.WaitGroup
	ready.Add(2)
	for m := 0; m < 2; m++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			stage := filepath.Join(cluster.mountPoints[m], dir)
			setupErr[m] = os.MkdirAll(stage, 0755)
			ready.Done()
			ready.Wait()
			if setupErr[m] != nil {
				return
			}
			for i, name := range names {
				p := filepath.Join(stage, name)
				if _, err := os.Lstat(p); err == nil {
					continue
				}
				wrote[m][i] = true
				if err := os.WriteFile(p, content[name], 0644); err != nil {
					failed[m] = append(failed[m], err.Error())
					continue
				}
				if err := os.Chmod(p, 0640); err != nil {
					failed[m] = append(failed[m], err.Error())
					continue
				}
				if err := os.Chtimes(p, mtime, mtime); err != nil {
					failed[m] = append(failed[m], err.Error())
				}
			}
		}(m)
	}
	wg.Wait()
	for m, f := range failed {
		require.NoError(t, setupErr[m], "mount%d", m)
		if len(f) > 0 {
			t.Logf("mount%d: %d calls failed, first: %s", m, len(f), f[0])
		}
	}
	both := 0
	for i := range names {
		if wrote[0][i] && wrote[1][i] {
			both++
		}
	}
	t.Logf("both mounts wrote %d of %d files", both, files)
	require.Positive(t, both, "the mounts never wrote the same file")

	// The filer deletes the chunks an update dropped shortly after the update.
	time.Sleep(5 * time.Second)

	conn, err := grpc.NewClient(cluster.filerGRPCAddress(0), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := filer_pb.NewSeaweedFilerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var broken []string
	for _, name := range names {
		if ctx.Err() != nil {
			broken = append(broken, "checks did not finish: "+ctx.Err().Error())
			break
		}
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/" + dir,
			Name:      name,
		})
		if err != nil {
			broken = append(broken, fmt.Sprintf("%s: lookup: %v", name, err))
			continue
		}
		for _, chunk := range resp.Entry.GetChunks() {
			if !cluster.needleExists(ctx, t, chunk.GetFileIdString()) {
				broken = append(broken, fmt.Sprintf("%s: chunk %s is deleted", name, chunk.GetFileIdString()))
			}
		}
		if got, err := cluster.readFromFiler(ctx, "/"+dir+"/"+name); err != nil {
			broken = append(broken, fmt.Sprintf("%s: read: %v", name, err))
		} else if !bytes.Equal(got, content[name]) {
			broken = append(broken, fmt.Sprintf("%s: read %d bytes, want the %d written", name, len(got), len(content[name])))
		}
	}
	require.Empty(t, broken, "%d of %d staged files are broken", len(broken), files)
}

// needleExists reports whether the volume server still has the needle; a
// deleted one answers 404 (only readDeleted=true would serve it).
func (c *dlmTestCluster) needleExists(ctx context.Context, t testing.TB, fid string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fmt.Sprintf("http://127.0.0.1:%d/%s", c.volumePort, fid), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// readFromFiler reads a file whole through filer0's HTTP API.
func (c *dlmTestCluster) readFromFiler(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", c.filerPorts[0], path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}
