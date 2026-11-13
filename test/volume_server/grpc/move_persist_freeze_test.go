package volume_server_grpc_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func moverServerAddress(cluster framework.MultiCluster, index int) pb.ServerAddress {
	return pb.ServerAddress(cluster.VolumeAdminAddress(index) + "." + strings.Split(cluster.VolumeGRPCAddress(index), ":")[1])
}

func reloadVolume(t *testing.T, client volume_server_pb.VolumeServerClient, volumeID uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.VolumeUnmount(ctx, &volume_server_pb.VolumeUnmountRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("unmount volume %d: %v", volumeID, err)
	}
	if _, err := client.VolumeMount(ctx, &volume_server_pb.VolumeMountRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("mount volume %d: %v", volumeID, err)
	}
}

func uploadStatus(t *testing.T, httpClient *http.Client, volumeURL string, volumeID uint32, key uint64, data string) int {
	t.Helper()
	resp := framework.UploadBytes(t, httpClient, volumeURL, framework.NewFileID(volumeID, key, 0x5EED0000+uint32(key)), []byte(data))
	framework.ReadAllAndClose(t, resp)
	return resp.StatusCode
}

func TestLiveMoveFreezeSurvivesSourceReload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMultiVolumeClusterAuto(t, matrix.P1(), 2)
	sourceConn, sourceClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	targetConn, targetClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer targetConn.Close()

	const volumeID = uint32(977)
	framework.AllocateVolume(t, sourceClient, volumeID, "")
	httpClient := framework.NewHTTPClient()
	if code := uploadStatus(t, httpClient, cluster.VolumeAdminURL(0), volumeID, 1, "before-move"); code != http.StatusCreated {
		t.Fatalf("initial upload: status %d", code)
	}

	var reloadedSourceWrite, targetWrite int
	mover := volume_move.NewMover(grpc.WithTransportCredentials(insecure.NewCredentials()))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := mover.LiveMoveVolume(ctx, needle.VolumeId(volumeID), moverServerAddress(cluster, 0), moverServerAddress(cluster, 1), volume_move.VolumeMoveOptions{
		IdleTimeout: time.Second,
		Progress: func(percent float64, stage string) {
			switch percent {
			case 20:
				// frozen, not yet copied: a reload must not reopen the source
				reloadVolume(t, sourceClient, volumeID)
				reloadedSourceWrite = uploadStatus(t, httpClient, cluster.VolumeAdminURL(0), volumeID, 2, "lost-after-move")
			case 70:
				// copied, not yet tailed: the copy must not inherit the freeze
				targetWrite = uploadStatus(t, httpClient, cluster.VolumeAdminURL(1), volumeID, 3, "on-target")
			}
		},
	})
	if err != nil {
		t.Fatalf("LiveMoveVolume: %v", err)
	}
	if reloadedSourceWrite != http.StatusConflict {
		t.Fatalf("reloaded source write: status %d, want %d", reloadedSourceWrite, http.StatusConflict)
	}
	if targetWrite != http.StatusCreated {
		t.Fatalf("copied target refused a write before the tail: status %d", targetWrite)
	}

	// The writable mark on the target is persisted too.
	reloadVolume(t, targetClient, volumeID)
	if code := uploadStatus(t, httpClient, cluster.VolumeAdminURL(1), volumeID, 4, "after-reload"); code != http.StatusCreated {
		t.Fatalf("moved volume refused a write after a reload: status %d", code)
	}
	for key, want := range map[uint64]string{1: "before-move", 3: "on-target", 4: "after-reload"} {
		resp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(1), framework.NewFileID(volumeID, key, 0x5EED0000+uint32(key)))
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusOK || string(body) != want {
			t.Fatalf("read key %d from target: status %d body %q, want %q", key, resp.StatusCode, body, want)
		}
	}
}

func TestLiveMoveAbortClearsPersistedFreeze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMultiVolumeClusterAuto(t, matrix.P1(), 2)
	sourceConn, sourceClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer sourceConn.Close()

	const volumeID = uint32(978)
	framework.AllocateVolume(t, sourceClient, volumeID, "")
	httpClient := framework.NewHTTPClient()
	if code := uploadStatus(t, httpClient, cluster.VolumeAdminURL(0), volumeID, 1, "before-move"); code != http.StatusCreated {
		t.Fatalf("initial upload: status %d", code)
	}

	mover := volume_move.NewMover(grpc.WithTransportCredentials(insecure.NewCredentials()))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := mover.LiveMoveVolume(ctx, needle.VolumeId(volumeID), moverServerAddress(cluster, 0), moverServerAddress(cluster, 1), volume_move.VolumeMoveOptions{
		Progress: func(percent float64, stage string) {
			if percent == 20 {
				cancel()
			}
		},
	})
	if err == nil {
		t.Fatalf("LiveMoveVolume succeeded with a cancelled context")
	}

	reloadVolume(t, sourceClient, volumeID)
	if code := uploadStatus(t, httpClient, cluster.VolumeAdminURL(0), volumeID, 2, "after-abort"); code != http.StatusCreated {
		t.Fatalf("aborted move left the source readonly after a reload: status %d", code)
	}
}
