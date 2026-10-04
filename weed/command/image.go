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
	Short:     "启动按需处理公开图片的独立网关",
	Long: `在公开 S3 对象入口前运行可选图片网关。x-oss-process 支持等比缩小、
绝对质量 quality,Q_85 及 JPEG/PNG/WebP 输出。原图权限在每次缓存读取前核验，
派生图只存入有容量上限的内存缓存，不写入 S3 或 Filer。
编码在独立 imgproxy 服务中进行，请为该服务配置像素、文件大小及容器资源限制。
这是公开图片入口，不接收 S3 签名或访问私有对象。`,
}

var imageOptions struct {
	source, imgproxy, bind          *string
	port, concurrency, maxDimension *int
	cacheMB, sourceMB, resultMB     *int64
	timeout                         *time.Duration
}

// init 注册独立入口的参数，不改变现有 S3、Filer 或 Volume 的行为。
func init() {
	cmdImage.Run = runImage
	imageOptions.source = cmdImage.Flag.String("source", "", "固定的匿名 S3 HTTP(S) 源地址，可包含桶路径")
	imageOptions.imgproxy = cmdImage.Flag.String("imgproxy", "", "独立 imgproxy HTTP(S) 服务地址")
	imageOptions.bind = cmdImage.Flag.String("ip.bind", "127.0.0.1", "监听地址")
	imageOptions.port = cmdImage.Flag.Int("port", 8334, "HTTP 端口")
	imageOptions.concurrency = cmdImage.Flag.Int("concurrency", 8, "最多同时处理的请求数，超出立即返回 429")
	imageOptions.maxDimension = cmdImage.Flag.Int("maxDimension", 4096, "允许的最大输出边长")
	imageOptions.cacheMB = cmdImage.Flag.Int64("cacheCapacityMB", 64, "派生图内存缓存容量，0 禁用")
	imageOptions.sourceMB = cmdImage.Flag.Int64("maxSourceMB", 25, "最大原图大小")
	imageOptions.resultMB = cmdImage.Flag.Int64("maxResultMB", 10, "最大处理结果大小")
	imageOptions.timeout = cmdImage.Flag.Duration("timeout", 15*time.Second, "单次请求的总超时")
}

// runImage 启动图片网关；签名材料从环境读取，避免出现在进程参数中。
func runImage(cmd *Command, args []string) bool {
	if *imageOptions.cacheMB < 0 || *imageOptions.cacheMB > 1<<20 ||
		*imageOptions.sourceMB < 1 || *imageOptions.sourceMB > 1024 ||
		*imageOptions.resultMB < 1 || *imageOptions.resultMB > 1024 {
		glog.Errorf("图片网关容量参数无效")
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
		glog.Errorf("图片网关配置无效: %v", err)
		return false
	}
	if *imageOptions.port < 1 || *imageOptions.port > 65535 {
		glog.Errorf("图片网关端口无效")
		return false
	}
	server := &http.Server{
		Addr: net.JoinHostPort(*imageOptions.bind, strconv.Itoa(*imageOptions.port)), Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, WriteTimeout: *imageOptions.timeout + time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	glog.V(0).Infof("图片网关监听 %s", server.Addr)
	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		glog.Errorf("图片网关启动失败: %v", err)
		return false
	}
	return true
}
