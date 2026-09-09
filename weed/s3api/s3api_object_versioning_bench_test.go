package s3api

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type listObjectVersionsBenchmarkFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entriesByDir map[string][]*filer_pb.Entry
	delay        time.Duration
}

func (f *listObjectVersionsBenchmarkFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	if f.delay > 0 && strings.HasSuffix(req.Directory, s3_constants.VersionsFolder) {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	entries := append([]*filer_pb.Entry(nil), f.entriesByDir[req.Directory]...)
	if req.Prefix != "" && req.Prefix != "/" {
		entries = filterBenchmarkEntries(entries, func(entry *filer_pb.Entry) bool {
			return strings.HasPrefix(entry.Name, req.Prefix)
		})
	}
	if req.StartFromFileName != "" {
		entries = filterBenchmarkEntries(entries, func(entry *filer_pb.Entry) bool {
			return entry.Name > req.StartFromFileName ||
				(req.InclusiveStartFrom && entry.Name == req.StartFromFileName)
		})
	}
	if req.Limit > 0 && len(entries) > int(req.Limit) {
		entries = entries[:req.Limit]
	}

	for _, entry := range entries {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func filterBenchmarkEntries(entries []*filer_pb.Entry, keep func(*filer_pb.Entry) bool) []*filer_pb.Entry {
	filtered := make([]*filer_pb.Entry, 0, len(entries))
	for _, entry := range entries {
		if keep(entry) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func newListObjectVersionsBenchmarkFiler(objectCount int, delay time.Duration) *listObjectVersionsBenchmarkFiler {
	const bucketDir = "/buckets/benchmark-bucket"

	entriesByDir := map[string][]*filer_pb.Entry{}
	rootEntries := make([]*filer_pb.Entry, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		objectName := fmt.Sprintf("object-%04d", i)
		versionsDirName := objectName + s3_constants.VersionsFolder
		rootEntries = append(rootEntries, &filer_pb.Entry{
			Name:        versionsDirName,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{},
			Extended: map[string][]byte{
				s3_constants.ExtLatestVersionIdKey: []byte("v1"),
			},
		})
		entriesByDir[bucketDir+"/"+versionsDirName] = []*filer_pb.Entry{{
			Name:       "v1",
			Attributes: &filer_pb.FuseAttributes{},
			Extended: map[string][]byte{
				s3_constants.ExtVersionIdKey: []byte("v1"),
				s3_constants.ExtETagKey:      []byte("\"benchmark-etag\""),
			},
		}}
	}
	sort.Slice(rootEntries, func(i, j int) bool {
		return rootEntries[i].Name < rootEntries[j].Name
	})
	entriesByDir[bucketDir] = rootEntries

	return &listObjectVersionsBenchmarkFiler{
		entriesByDir: entriesByDir,
		delay:        delay,
	}
}

func newListObjectVersionsBenchmarkServer(b *testing.B, objectCount int, delay time.Duration) *S3ApiServer {
	b.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	filer := newListObjectVersionsBenchmarkFiler(objectCount, delay)
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, filer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	filerAddress := pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", port))
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath:    "/buckets",
			Filers:         []pb.ServerAddress{filerAddress},
			GrpcDialOption: dialOption,
		},
		filerClient: wdclient.NewFilerClient([]pb.ServerAddress{filerAddress}, dialOption, ""),
	}

	b.Cleanup(func() {
		pb.InvalidateGrpcConnection(filerAddress.ToGrpcAddress())
		grpcServer.Stop()
		_ = listener.Close()
	})
	return s3a
}

// BenchmarkListObjectVersionsSerial measures the current serial traversal of
// independent .versions directories. The injected delay models a remote Filer
// round trip and makes the cost of waiting for each directory observable.
func BenchmarkListObjectVersionsSerial(b *testing.B) {
	for _, objectCount := range []int{8, 32, 128} {
		for _, delay := range []time.Duration{0, time.Millisecond} {
			name := fmt.Sprintf("objects=%d/delay=%s", objectCount, delay)
			b.Run(name, func(b *testing.B) {
				s3a := newListObjectVersionsBenchmarkServer(b, objectCount, delay)

				b.StopTimer()
				result, err := s3a.listObjectVersions("benchmark-bucket", "", "", "", "", 1000)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Entries) != objectCount {
					b.Fatalf("warm-up returned %d entries, want %d", len(result.Entries), objectCount)
				}
				b.StartTimer()

				for i := 0; i < b.N; i++ {
					result, err := s3a.listObjectVersions("benchmark-bucket", "", "", "", "", 1000)
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Entries) != objectCount {
						b.Fatalf("returned %d entries, want %d", len(result.Entries), objectCount)
					}
				}
			})
		}
	}
}
