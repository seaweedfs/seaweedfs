package volume_server_http_test

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func makePNGFixture(t testing.TB, width, height int) []byte {
	t.Helper()

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x * 20), G: uint8(y * 20), B: 200, A: 255})
		}
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png fixture: %v", err)
	}
	return buf.Bytes()
}

func decodeImageConfig(t testing.TB, data []byte) image.Config {
	t.Helper()
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode image config: %v", err)
	}
	return cfg
}

func TestImageResizeAndCropReadVariants(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(101)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fullFileID := framework.NewFileID(volumeID, 772004, 0x4D5E6F70)
	uploadReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fullFileID, makePNGFixture(t, 6, 4))
	uploadReq.Header.Set("Content-Type", "image/png")
	uploadResp := framework.DoRequest(t, client, uploadReq)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("image upload expected 201, got %d", uploadResp.StatusCode)
	}

	parts := strings.SplitN(fullFileID, ",", 2)
	if len(parts) != 2 {
		t.Fatalf("unexpected file id format: %q", fullFileID)
	}
	fidOnly := parts[1]

	resizeURL := fmt.Sprintf("%s/%d/%s/%s?width=2&height=1", clusterHarness.VolumeAdminURL(), volumeID, fidOnly, "fixture.png")
	resizeResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, resizeURL))
	resizeBody := framework.ReadAllAndClose(t, resizeResp)
	if resizeResp.StatusCode != http.StatusOK {
		t.Fatalf("image resize read expected 200, got %d", resizeResp.StatusCode)
	}
	resizeCfg := decodeImageConfig(t, resizeBody)
	if resizeCfg.Width > 2 || resizeCfg.Height > 1 {
		t.Fatalf("image resize expected dimensions <= 2x1, got %dx%d", resizeCfg.Width, resizeCfg.Height)
	}

	cropURL := fmt.Sprintf("%s/%d/%s/%s?crop_x1=1&crop_y1=1&crop_x2=4&crop_y2=3", clusterHarness.VolumeAdminURL(), volumeID, fidOnly, "fixture.png")
	cropResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cropURL))
	cropBody := framework.ReadAllAndClose(t, cropResp)
	if cropResp.StatusCode != http.StatusOK {
		t.Fatalf("image crop read expected 200, got %d", cropResp.StatusCode)
	}
	cropCfg := decodeImageConfig(t, cropBody)
	if cropCfg.Width != 3 || cropCfg.Height != 2 {
		t.Fatalf("image crop expected 3x2, got %dx%d", cropCfg.Width, cropCfg.Height)
	}
}
