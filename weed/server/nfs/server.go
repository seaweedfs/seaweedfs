package nfs

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	gonfs "github.com/willscott/go-nfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	// PortmapBind, when non-empty, enables a built-in portmap v2 responder
	// on <PortmapBind>:111 advertising the NFS v3 and MOUNT v3 services at
	// Port. Empty (the default) disables portmap; clients must then bypass
	// portmap with mount -o port=,mountport=,proto=tcp,mountproto=tcp.
	PortmapBind string
}

type Server struct {
	option             *Option
	exportRoot         util.FullPath
	exportID           uint32
	signature          int32
	handleLimit        int
	clientAuthorizer   *clientAuthorizer
	sharedReaderCache  *filer.ReaderCache
	chunkInvalidator   chunkInvalidator
	filerClient        *wdclient.FilerClient
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
	if option.GrpcDialOption == nil {
		option.GrpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	clientAuthorizer, err := newClientAuthorizer(option.AllowedClients)
	if err != nil {
		return nil, err
	}
	var filerClient *wdclient.FilerClient
	if option.VolumeServerAccess != "filerProxy" {
		var opts *wdclient.FilerClientOption
		if option.VolumeServerAccess == "publicUrl" {
			opts = &wdclient.FilerClientOption{UrlPreference: wdclient.PreferPublicUrl}
		}
		filerClient = wdclient.NewFilerClient([]pb.ServerAddress{option.Filer}, option.GrpcDialOption, "", opts)
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
		filerClient:        filerClient,
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

	var portmap *portmapServer
	if s.option.PortmapBind != "" {
		portmap = newPortmapServer(s.option.PortmapBind, portmapPort, uint32(s.option.Port))
		if pmErr := portmap.Start(); pmErr != nil {
			_ = listener.Close()
			return fmt.Errorf("start portmap: %w", pmErr)
		}
		glog.V(0).Infof("NFS portmap responder listening on %s:%d (NFS v3 tcp=%d, MOUNT v3 tcp=%d)",
			s.option.PortmapBind, portmapPort, s.option.Port, s.option.Port)
		defer func() {
			if portmap != nil {
				_ = portmap.Close()
			}
		}()
	}

	s.logMountHint()
	return s.serve(listener)
}

// logMountHint prints a copy-pasteable Linux mount command so operators can
// see at startup how to mount the export from a client. The go-nfs library
// does not run portmap, so without -portmap.bind the client must bypass
// portmap via -o port=,mountport=,proto=tcp,mountproto=tcp.
func (s *Server) logMountHint() {
	exportPath := string(s.exportRoot)
	if s.option.PortmapBind != "" {
		glog.V(0).Infof("mount example: mount -t nfs -o nfsvers=3,nolock <host>:%s <mountpoint>", exportPath)
		return
	}
	glog.V(0).Infof("mount example (bypasses portmap): mount -t nfs -o nfsvers=3,nolock,noacl,port=%d,mountport=%d,proto=tcp,mountproto=tcp <host>:%s <mountpoint>",
		s.option.Port, s.option.Port, exportPath)
	glog.V(0).Infof("tip: pass -portmap.bind to enable the built-in portmap responder on port 111 so plain `mount -t nfs host:%s /mnt` works.", exportPath)
}

func (s *Server) serve(listener net.Listener) error {
	if s.filerClient != nil {
		defer s.filerClient.Close()
	}
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
	followCtx, followCancel := context.WithCancel(context.Background())
	defer followCancel()
	followDone := make(chan struct{})
	go func() {
		defer close(followDone)
		s.runMetadataInvalidationLoop(followCtx)
	}()
	defer func() {
		followCancel()
		<-followDone
	}()

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
	rootFS := newSeaweedFileSystem(s, s.exportRoot, s.sharedReaderCache)
	if s.sharedReaderCache == nil {
		s.sharedReaderCache = rootFS.readerCache
	}
	if s.chunkInvalidator == nil {
		s.chunkInvalidator = s.sharedReaderCache
	}
	return &Handler{
		server: s,
		rootFS: rootFS,
	}, nil
}

func (s *Server) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	if s == nil || s.withFilerClient == nil {
		return errors.New("nfs filer client is not configured")
	}
	return s.withFilerClient(streamingMode, fn)
}

func (s *Server) LookupFn() wdclient.LookupFileIdFunctionType {
	if s == nil {
		return nil
	}
	if s.option != nil && s.option.VolumeServerAccess == "filerProxy" {
		return func(ctx context.Context, fileID string) ([]string, error) {
			return []string{fmt.Sprintf("http://%s/?proxyChunkId=%s", s.option.Filer.ToHttpAddress(), fileID)}, nil
		}
	}
	if s.filerClient != nil {
		return s.filerClient.GetLookupFileIdFunction()
	}
	return nil
}
