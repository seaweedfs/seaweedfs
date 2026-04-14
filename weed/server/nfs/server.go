package nfs

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

var ErrNotImplemented = errors.New("nfs protocol serving is not implemented yet")

type Option struct {
	Filer              pb.ServerAddress
	BindIp             string
	Port               int
	FilerRootPath      string
	VolumeServerAccess string
	GrpcDialOption     grpc.DialOption
}

type Server struct {
	option     *Option
	exportRoot util.FullPath
	exportID   uint32
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
	exportRoot := normalizeExportRoot(util.FullPath(option.FilerRootPath))
	return &Server{
		option:     option,
		exportRoot: exportRoot,
		exportID:   exportIDForRoot(exportRoot),
	}, nil
}

func (s *Server) Start() error {
	return fmt.Errorf("%w; filer=%s bind=%s port=%d filer.path=%s exportId=%d volumeServerAccess=%s",
		ErrNotImplemented,
		s.option.Filer,
		s.option.BindIp,
		s.option.Port,
		s.exportRoot,
		s.exportID,
		s.option.VolumeServerAccess,
	)
}
