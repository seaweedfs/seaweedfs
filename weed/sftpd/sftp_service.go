// sftp_service.go
package sftpd

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/auth"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
)

// SFTPService holds configuration for the SFTP service.
type SFTPService struct {
	options     SFTPServiceOptions
	userStore   user.Store
	authManager *auth.Manager
}

// SFTPServiceOptions contains all configuration options for the SFTP service.
type SFTPServiceOptions struct {
	GrpcDialOption grpc.DialOption
	DataCenter     string
	FilerGroup     string
	Filer          pb.ServerAddress

	// SSH Configuration
	SshPrivateKey  string        // Legacy single host key
	HostKeysFolder string        // Multiple host keys for different algorithms
	AuthMethods    []string      // Enabled auth methods: "password", "publickey", "keyboard-interactive"
	MaxAuthTries   int           // Limit authentication attempts
	BannerMessage  string        // Pre-auth banner message
	LoginGraceTime time.Duration // Timeout for authentication

	// Connection Management
	ClientAliveInterval time.Duration // Keep-alive check interval
	ClientAliveCountMax int           // Max missed keep-alives before disconnect

	// User Management
	UserStoreFile string // Path to user store file
}

// NewSFTPService creates a new service instance.
func NewSFTPService(options *SFTPServiceOptions) *SFTPService {
	service := SFTPService{options: *options}

	// Initialize user store
	userStore, err := user.NewFileStore(options.UserStoreFile)
	if err != nil {
		glog.Fatalf("Failed to initialize user store: %v", err)
	}
	service.userStore = userStore

	// Initialize auth manager
	service.authManager = auth.NewManager(userStore, options.AuthMethods)

	return &service
}

// Serve accepts incoming connections on the provided listener and handles them.
func (s *SFTPService) Serve(listener net.Listener) error {
	// Build SSH server config
	sshConfig, err := s.buildSSHConfig()
	if err != nil {
		return fmt.Errorf("failed to create SSH config: %v", err)
	}

	glog.V(0).Infof("Starting Seaweed SFTP service on %s", listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept incoming connection: %v", err)
		}
		go s.handleSSHConnection(conn, sshConfig)
	}
}

// buildSSHConfig creates the SSH server configuration with proper authentication.
func (s *SFTPService) buildSSHConfig() (*ssh.ServerConfig, error) {
	// Get base config from auth manager
	config := s.authManager.GetSSHServerConfig()

	// Set additional options
	config.MaxAuthTries = s.options.MaxAuthTries
	config.BannerCallback = func(conn ssh.ConnMetadata) string {
		return s.options.BannerMessage
	}
	config.ServerVersion = "SSH-2.0-SeaweedFS-SFTP" // Custom server version

	hostKeysAdded := 0
	// Add legacy host key if specified
	if s.options.SshPrivateKey != "" {
		if err := s.addHostKey(config, s.options.SshPrivateKey); err != nil {
			return nil, err
		}
		hostKeysAdded++
	}

	// Add all host keys from the specified folder
	if s.options.HostKeysFolder != "" {
		files, err := os.ReadDir(s.options.HostKeysFolder)
		if err != nil {
			return nil, fmt.Errorf("failed to read host keys folder: %v", err)
		}
		for _, file := range files {
			if file.IsDir() {
				continue // Skip directories
			}

			keyPath := filepath.Join(s.options.HostKeysFolder, file.Name())
			if err := s.addHostKey(config, keyPath); err != nil {
				// Log the error but continue with other keys
				glog.V(0).Info(fmt.Sprintf("Failed to add host key %s: %v", keyPath, err))
				continue
			}
			hostKeysAdded++
		}

		if hostKeysAdded == 0 {
			glog.V(0).Info(fmt.Sprintf("Warning: no valid host keys found in folder %s", s.options.HostKeysFolder))
		}
	}

	// Ensure we have at least one host key
	if hostKeysAdded == 0 {
		return nil, fmt.Errorf("no host keys provided")
	}
	return config, nil
}

// addHostKey adds a host key to the SSH server configuration.
func (s *SFTPService) addHostKey(config *ssh.ServerConfig, keyPath string) error {
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("failed to read host key %s: %v", keyPath, err)
	}

	// Try parsing as private key
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		// Try parsing with passphrase if available
		if passphraseErr, ok := err.(*ssh.PassphraseMissingError); ok {
			return fmt.Errorf("host key %s requires passphrase: %v", keyPath, passphraseErr)
		}
		return fmt.Errorf("failed to parse host key %s: %v", keyPath, err)
	}
	config.AddHostKey(signer)
	glog.V(0).Infof("Added host key %s (%s)", keyPath, signer.PublicKey().Type())
	return nil
}

// handleSSHConnection handles an incoming SSH connection.
func (s *SFTPService) handleSSHConnection(conn net.Conn, config *ssh.ServerConfig) {
	// Set connection deadline for handshake
	_ = conn.SetDeadline(time.Now().Add(s.options.LoginGraceTime))

	// Perform SSH handshake
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, config)
	if err != nil {
		glog.Errorf("Failed to handshake: %v", err)
		conn.Close()
		return
	}

	// Clear deadline after successful handshake
	_ = conn.SetDeadline(time.Time{})

	// Set up connection monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start keep-alive monitoring
	go s.monitorConnection(ctx, sshConn)

	username := sshConn.Permissions.Extensions["username"]
	glog.V(0).Infof("New SSH connection from %s (%s) as user %s",
		sshConn.RemoteAddr(), sshConn.ClientVersion(), username)

	// Get user from store
	sftpUser, err := s.authManager.GetUser(username)
	if err != nil {
		glog.Errorf("Failed to retrieve user %s: %v", username, err)
		sshConn.Close()
		return
	}

	// Create user-specific filesystem
	userFs := NewSftpServer(
		s.options.Filer,
		s.options.GrpcDialOption,
		s.options.DataCenter,
		s.options.FilerGroup,
		sftpUser,
	)

	// Ensure home directory exists with proper permissions
	if err := userFs.EnsureHomeDirectory(); err != nil {
		glog.Errorf("Failed to ensure home directory for user %s: %v", username, err)
		// We don't close the connection here, as the user might still be able to access other directories
	}

	// Handle SSH requests and channels
	go ssh.DiscardRequests(reqs)
	for newChannel := range chans {
		go s.handleChannel(newChannel, &userFs)
	}
}

// monitorConnection monitors an SSH connection with keep-alives.
func (s *SFTPService) monitorConnection(ctx context.Context, sshConn *ssh.ServerConn) {
	if s.options.ClientAliveInterval <= 0 {
		return
	}

	ticker := time.NewTicker(s.options.ClientAliveInterval)
	defer ticker.Stop()

	missedCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Send keep-alive request
			_, _, err := sshConn.SendRequest("keepalive@openssh.com", true, nil)
			if err != nil {
				missedCount++
				glog.V(0).Infof("Keep-alive missed for %s: %v (%d/%d)",
					sshConn.RemoteAddr(), err, missedCount, s.options.ClientAliveCountMax)

				if missedCount >= s.options.ClientAliveCountMax {
					glog.Warningf("Closing unresponsive connection from %s", sshConn.RemoteAddr())
					sshConn.Close()
					return
				}
			} else {
				missedCount = 0
			}
		}
	}
}

// handleChannel handles a single SSH channel.
func (s *SFTPService) handleChannel(newChannel ssh.NewChannel, fs *SftpServer) {
	if newChannel.ChannelType() != "session" {
		_ = newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
		return
	}

	channel, requests, err := newChannel.Accept()
	if err != nil {
		glog.Errorf("Could not accept channel: %v", err)
		return
	}

	go func(in <-chan *ssh.Request) {
		for req := range in {
			switch req.Type {
			case "subsystem":
				// Check that the subsystem is "sftp".
				if string(req.Payload[4:]) == "sftp" {
					_ = req.Reply(true, nil)
					s.handleSFTP(channel, fs)
				} else {
					_ = req.Reply(false, nil)
				}
			default:
				_ = req.Reply(false, nil)
			}
		}
	}(requests)
}

// handleSFTP starts the SFTP server on the SSH channel.
func (s *SFTPService) handleSFTP(channel ssh.Channel, fs *SftpServer) {
	// Create server options with initial working directory set to user's home
	serverOptions := sftp.WithStartDirectory(fs.user.HomeDir)
	server := sftp.NewRequestServer(channel, sftp.Handlers{
		FileGet:  fs,
		FilePut:  fs,
		FileCmd:  fs,
		FileList: fs,
	}, serverOptions)

	if err := server.Serve(); err == io.EOF {
		server.Close()
		glog.V(0).Info("SFTP client exited session.")
	} else if err != nil {
		glog.Errorf("SFTP server finished with error: %v", err)
	}
}
