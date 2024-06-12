package ftpd

import (
	"crypto/tls"
	"errors"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"net"

	ftpserver "github.com/fclairamb/ftpserverlib"
	"google.golang.org/grpc"
)

type FtpServerOption struct {
	Filer            string
	IP               string
	IpBind           string
	Port             int
	FilerGrpcAddress string
	FtpRoot          string
	GrpcDialOption   grpc.DialOption
	PassivePortStart int
	PassivePortStop  int
}

type SftpServer struct {
	option      *FtpServerOption
	ftpListener net.Listener
}

var _ = ftpserver.MainDriver(&SftpServer{})

// NewFtpServer returns a new FTP server driver
func NewFtpServer(ftpListener net.Listener, option *FtpServerOption) (*SftpServer, error) {
	var err error
	server := &SftpServer{
		option:      option,
		ftpListener: ftpListener,
	}
	return server, err
}

// GetSettings returns some general settings around the server setup
func (s *SftpServer) GetSettings() (*ftpserver.Settings, error) {
	var portRange *ftpserver.PortRange
	if s.option.PassivePortStart > 0 && s.option.PassivePortStop > s.option.PassivePortStart {
		portRange = &ftpserver.PortRange{
			Start: s.option.PassivePortStart,
			End:   s.option.PassivePortStop,
		}
	}

	return &ftpserver.Settings{
		Listener:                 s.ftpListener,
		ListenAddr:               util.JoinHostPort(s.option.IpBind, s.option.Port),
		PublicHost:               s.option.IP,
		PassiveTransferPortRange: portRange,
		ActiveTransferPortNon20:  true,
		IdleTimeout:              -1,
		ConnectionTimeout:        20,
	}, nil
}

// ClientConnected is called to send the very first welcome message
func (s *SftpServer) ClientConnected(cc ftpserver.ClientContext) (string, error) {
	return "Welcome to SeaweedFS FTP Server", nil
}

// ClientDisconnected is called when the user disconnects, even if he never authenticated
func (s *SftpServer) ClientDisconnected(cc ftpserver.ClientContext) {
}

// AuthUser authenticates the user and selects an handling driver
func (s *SftpServer) AuthUser(cc ftpserver.ClientContext, username, password string) (ftpserver.ClientDriver, error) {
	return nil, nil
}

// GetTLSConfig returns a TLS Certificate to use
// The certificate could frequently change if we use something like "let's encrypt"
func (s *SftpServer) GetTLSConfig() (*tls.Config, error) {
	return nil, errors.New("no TLS certificate configured")
}
