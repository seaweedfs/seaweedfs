package udm

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/seaweedfs/seaweedfs/weed/storage/backend/udm/api/v1"
)

type server struct {
	pb.UnimplementedUDMStorageServer
}

const (
	prefix    = "/Users/lou/tmp/udm"
	cacheRoot = "/Users/lou/tmp/cache"
)

func (s *server) UploadFile(req *pb.FileRequest, stream pb.UDMStorage_UploadFileServer) error {
	fmt.Printf("upload %s to %s\n", req.File, req.Key)

	f, err := os.Open(req.File)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file %q, %v", req.File, err)
	}

	fileSize := info.Size()

	targetPath := filepath.Join(prefix, req.Key)
	err = os.MkdirAll(filepath.Dir(targetPath), 0755)
	if err != nil {
		return err
	}
	target, err := os.Create(targetPath)
	if err != nil {
		return err
	}

	defer target.Close()

	_, err = io.Copy(target, f)
	if err != nil {
		return err
	}

	return stream.Send(&pb.FileInfo{
		TotalBytes: fileSize,
		Percentage: 100,
	})
}

func (s *server) DownloadFile(req *pb.FileRequest, stream pb.UDMStorage_DownloadFileServer) error {
	fmt.Printf("download %s to %s\n", req.Key, req.File)

	cacheFile, exists := getFileInCache(req.Key)
	if exists {
		f, err := os.Open(cacheFile)
		if err != nil {
			return err
		}

		defer f.Close()

		source, err := os.Create(req.File)
		if err != nil {
			return err
		}

		defer source.Close()

		n, err := io.Copy(source, f)
		if err != nil {
			return err
		}

		return stream.Send(&pb.FileInfo{
			TotalBytes: n,
			Percentage: 100,
		})
	}

	targetPath := filepath.Join(prefix, req.Key)
	f, err := os.Open(targetPath)
	if err != nil {
		return err
	}

	defer f.Close()

	source, err := os.Create(req.File)
	if err != nil {
		return err
	}

	defer source.Close()

	n, err := io.Copy(source, f)
	if err != nil {
		return err
	}

	return stream.Send(&pb.FileInfo{
		TotalBytes: n,
		Percentage: 100,
	})
}

func (s *server) DeleteFile(ctx context.Context, req *pb.FileKey) (*emptypb.Empty, error) {
	targetPath := filepath.Join(prefix, req.Key)

	log.Printf("delete %s\n", targetPath)

	err := deleteFileInCache(req.Key)
	if err != nil {
		fmt.Printf("failed to delete %s in cache, %v", req.Key, err)
	}

	return &emptypb.Empty{}, os.Remove(targetPath)
}

func (s *server) CacheFile(ctx context.Context, req *pb.FileKey) (*pb.CacheFileReply, error) {
	cacheFile, exists := getFileInCache(req.Key)
	if exists {
		return &pb.CacheFileReply{
			CacheFile: cacheFile,
		}, nil
	}

	targetPath := filepath.Join(prefix, req.Key)
	f, err := os.Open(targetPath)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	cf, err := createCacheFile(req.Key)
	if err != nil {
		return nil, err
	}

	defer cf.Close()

	_, err = io.Copy(cf, f)
	if err != nil {
		return nil, err
	}

	return &pb.CacheFileReply{
		CacheFile: cf.Name(),
	}, nil
}

func NewServer(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer()
	pb.RegisterUDMStorageServer(s, &server{})

	log.Printf("Server listening at %v\n", lis.Addr())

	return s.Serve(lis)
}

func getFileInCache(key string) (string, bool) {
	path := filepath.Join(cacheRoot, key)
	if _, err := os.Stat(path); err == nil {
		return path, true
	}

	return "", false
}

func deleteFileInCache(key string) error {
	path := filepath.Join(cacheRoot, key)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func createCacheFile(key string) (*os.File, error) {
	path := filepath.Join(cacheRoot, key)
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return nil, err
	}

	return os.Create(path)
}
