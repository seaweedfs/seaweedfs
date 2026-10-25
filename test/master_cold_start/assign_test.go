package master_cold_start

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// The very first write against a fresh cluster must succeed: the assign that
// triggers volume growth waits for it, and when growth outlasts that wait the
// master sheds with a retryable 503/Retry-After. A normal client honors the
// hint and retries, so the test does the same within a deadline.
func TestColdStartFirstWrite(t *testing.T) {
	c := StartCluster(t, 3)

	// Each subtest uses its own replication so its volume layout starts cold.
	t.Run("http_assign_and_write", func(t *testing.T) {
		client := &http.Client{Timeout: 15 * time.Second}
		var assign struct {
			Fid   string `json:"fid"`
			Url   string `json:"url"`
			Error string `json:"error"`
		}
		deadline := time.Now().Add(60 * time.Second)
		for {
			resp, err := client.Get(c.MasterURL() + "/dir/assign?replication=002")
			if err != nil {
				t.Fatalf("dir/assign: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable && time.Now().Before(deadline) {
				time.Sleep(time.Second)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("dir/assign returned %d: %s", resp.StatusCode, body)
			}
			if err := json.Unmarshal(body, &assign); err != nil {
				t.Fatalf("parse assign response %q: %v", body, err)
			}
			if assign.Error != "" || assign.Fid == "" {
				t.Fatalf("assign failed: %s", body)
			}
			break
		}

		content := []byte("cold start write")
		if err := uploadAndReadBack(client, assign.Url, assign.Fid, content); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("grpc_assign", func(t *testing.T) {
		grpcDialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
		var err error
		for deadline := time.Now().Add(60 * time.Second); ; {
			err = pb.WithMasterClient(context.Background(), false, pb.ServerAddress(c.MasterAddress()), grpcDialOption, false,
				func(client master_pb.SeaweedClient) error {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					resp, err := client.Assign(ctx, &master_pb.AssignRequest{Count: 1, Replication: "001"})
					if err != nil {
						return err
					}
					if resp.Error != "" || resp.Fid == "" {
						return fmt.Errorf("assign failed: %+v", resp)
					}
					return nil
				})
			if err == nil || status.Code(err) != codes.ResourceExhausted || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			t.Fatalf("grpc assign on cold cluster: %v", err)
		}
	})
}

// uploadAndReadBack writes content to the assigned fid and reads it back.
func uploadAndReadBack(client *http.Client, volumeUrl, fid string, content []byte) error {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("file", "cold_start.txt")
	if err != nil {
		return err
	}
	part.Write(content)
	writer.Close()

	target := fmt.Sprintf("http://%s/%s", volumeUrl, fid)
	req, err := http.NewRequest(http.MethodPost, target, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("upload to %s: %w", target, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload to %s returned %d: %s", target, resp.StatusCode, body)
	}

	resp, err = client.Get(target)
	if err != nil {
		return fmt.Errorf("read back %s: %w", target, err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("read back %s returned %d", target, resp.StatusCode)
	}
	if !bytes.Equal(got, content) {
		return fmt.Errorf("read back %s: got %q, want %q", target, got, content)
	}
	return nil
}
