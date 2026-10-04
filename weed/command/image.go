package command

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/images/gateway"
)

var cmdImage = &Command{
	UsageLine: "image -source=http://localhost:8333/public-bucket -imgproxy=http://localhost:8080",
	Short:     "Start an on-demand gateway for public images",
	Long: `Run an optional image gateway in front of public S3 objects. x-oss-process
supports aspect-preserving downscaling, absolute quality,Q_85, and JPEG/PNG/WebP.
Source access is checked before every cache read. Processed images remain in a
bounded memory cache and are never written to S3 or the Filer.
Encoding runs in a separate imgproxy service; configure its pixel, file size,
and process resource limits. This public endpoint does not accept S3 signatures
or read private objects.`,
}

var imageOptions struct {
	source, imgproxy, bind          *string
	port, concurrency, maxDimension *int
	cacheMB, sourceMB, resultMB     *int64
	timeout                         *time.Duration
}

// init registers the optional endpoint without changing S3, Filer, or Volume services.
func init() {
	cmdImage.Run = runImage
	imageOptions.source = cmdImage.Flag.String("source", "", "Fixed anonymous S3 HTTP(S) source URL, optionally with a bucket path")
	imageOptions.imgproxy = cmdImage.Flag.String("imgproxy", "", "Separate imgproxy HTTP(S) service URL")
	imageOptions.bind = cmdImage.Flag.String("ip.bind", "127.0.0.1", "Listen address")
	imageOptions.port = cmdImage.Flag.Int("port", 8334, "HTTP port")
	imageOptions.concurrency = cmdImage.Flag.Int("concurrency", 8, "Maximum concurrent requests and encoding jobs; excess requests return 429")
	imageOptions.maxDimension = cmdImage.Flag.Int("maxDimension", 4096, "Maximum output dimension")
	imageOptions.cacheMB = cmdImage.Flag.Int64("cacheCapacityMB", 64, "Processed image memory cache in MiB; 0 disables caching")
	imageOptions.sourceMB = cmdImage.Flag.Int64("maxSourceMB", 25, "Maximum source image size in MiB")
	imageOptions.resultMB = cmdImage.Flag.Int64("maxResultMB", 10, "Maximum processed image size in MiB")
	imageOptions.timeout = cmdImage.Flag.Duration("timeout", 15*time.Second, "Separate timeout budgets for source metadata, shared encoding, and client writes")
}

// runImage starts the gateway, reading signing material from the environment rather than process arguments.
func runImage(cmd *Command, args []string) bool {
	if *imageOptions.cacheMB < 0 || *imageOptions.cacheMB > 1<<20 ||
		*imageOptions.sourceMB < 1 || *imageOptions.sourceMB > 1024 ||
		*imageOptions.resultMB < 1 || *imageOptions.resultMB > 1024 {
		glog.Errorf("Invalid image gateway capacity options")
		return false
	}
	handler, err := gateway.New(gateway.Config{
		Source: *imageOptions.source, Imgproxy: *imageOptions.imgproxy,
		Key: os.Getenv("IMGPROXY_KEY"), Salt: os.Getenv("IMGPROXY_SALT"),
		Concurrency: *imageOptions.concurrency, MaxDimension: *imageOptions.maxDimension,
		CacheBytes: *imageOptions.cacheMB << 20, MaxSourceBytes: *imageOptions.sourceMB << 20,
		MaxResultBytes: *imageOptions.resultMB << 20, Timeout: *imageOptions.timeout,
	})
	if err != nil {
		glog.Errorf("Invalid image gateway configuration: %v", err)
		return false
	}
	if *imageOptions.port < 1 || *imageOptions.port > 65535 {
		glog.Errorf("Invalid image gateway port")
		return false
	}
	// Bound the initial source check, shared encoding, and response write phases.
	// ReadTimeout also limits draining of ignored request bodies after the handler returns.
	server := &http.Server{
		Addr: net.JoinHostPort(*imageOptions.bind, strconv.Itoa(*imageOptions.port)), Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 3 * *imageOptions.timeout,
		IdleTimeout:  60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	glog.V(0).Infof("Image gateway listening on %s", server.Addr)
	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		glog.Errorf("Image gateway failed: %v", err)
		return false
	}
	return true
}
