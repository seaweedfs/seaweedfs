package nfs

import (
	"errors"
	"fmt"
	"net"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	gonfs "github.com/willscott/go-nfs"
	"google.golang.org/grpc"
)

type Option struct {
	Filer              pb.ServerAddress
	BindIp             string
	Port               int
	FilerRootPath      string
	ReadOnly           bool
	AllowedClients     []string
	VolumeServerAccess string
	GrpcDialOption     grpc.DialOption
}

type Server struct {
	option             *Option
	exportRoot         util.FullPath
	exportID           uint32
	signature          int32
	handleLimit        int
	clientAuthorizer   *clientAuthorizer
	newUploader        func() (chunkUploader, error)
	withFilerClient    filerClientExecutor
	withInternalClient internalClientExecutor
}

func NewServer(option *Option) (*Server, error) {
	if option == nil {
		return nil, errors.New("nfs option is required")
	}
	if option.Port <= 0 {
		return nil, fmt.Errorf("nfs port must be positive: %d", option.Port)
	}
	if option.FilerRootPath == "" {
		option.FilerRootPath = "/"
	}
	if option.VolumeServerAccess == "" {
		option.VolumeServerAccess = "direct"
	}
	clientAuthorizer, err := newClientAuthorizer(option.AllowedClients)
	if err != nil {
		return nil, err
	}
	exportRoot := normalizeExportRoot(util.FullPath(option.FilerRootPath))
	signature := util.RandomInt32()
	return &Server{
		option:             option,
		exportRoot:         exportRoot,
		exportID:           exportIDForRoot(exportRoot),
		signature:          signature,
		handleLimit:        1 << 20,
		clientAuthorizer:   clientAuthorizer,
		newUploader:        newChunkUploader,
		withFilerClient:    newFilerClientExecutor(option, signature),
		withInternalClient: newInternalClientExecutor(option, signature),
	}, nil
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.option.BindIp, s.option.Port))
	if err != nil {
		return fmt.Errorf("listen nfs on %s:%d: %w", s.option.BindIp, s.option.Port, err)
	}

	return s.serve(listener)
}

func (s *Server) serve(listener net.Listener) error {
	if s.clientAuthorizer != nil && s.clientAuthorizer.enabled {
		listener = &allowlistListener{
			Listener:   listener,
			authorizer: s.clientAuthorizer,
		}
	}

	handler, err := s.newHandler()
	if err != nil {
		_ = listener.Close()
		return err
	}

	glog.V(0).Infof("Start Seaweed NFS Server filer=%s bind=%s export=%s exportId=%d readOnly=%t allowedClients=%d volumeServerAccess=%s",
		s.option.Filer,
		listener.Addr(),
		s.exportRoot,
		s.exportID,
		s.option.ReadOnly,
		len(s.option.AllowedClients),
		s.option.VolumeServerAccess,
	)

	return gonfs.Serve(listener, handler)
}

func (s *Server) newHandler() (*Handler, error) {
	if s == nil {
		return nil, errors.New("nfs server is not configured")
	}
	return &Handler{
		server: s,
		rootFS: newSeaweedFileSystem(s, s.exportRoot, nil),
	}, nil
}

func (s *Server) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	if s == nil || s.withFilerClient == nil {
		return errors.New("nfs filer client is not configured")
	}
	return s.withFilerClient(streamingMode, fn)
}
