package command

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"github.com/spf13/viper"

	"github.com/seaweedfs/seaweedfs/weed/admin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/handlers"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc" // Register filer_etc credential store
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

var (
	a AdminOptions
)

type AdminOptions struct {
	port             *int
	grpcPort         *int
	master           *string
	masters          *string // deprecated, for backward compatibility
	adminUser        *string
	adminPassword    *string
	readOnlyUser     *string
	readOnlyPassword *string
	dataDir          *string
	icebergPort      *int
	urlPrefix        *string
	debug            *bool
	debugPort        *int
	cpuProfile       *string
	memProfile       *string
}

func init() {
	cmdAdmin.Run = runAdmin // break init cycle
	a.port = cmdAdmin.Flag.Int("port", 23646, "admin server port")
	a.grpcPort = cmdAdmin.Flag.Int("port.grpc", 0, "gRPC server port for worker connections (default: http port + 10000)")
	a.master = cmdAdmin.Flag.String("master", "localhost:9333", "comma-separated master servers")
	a.masters = cmdAdmin.Flag.String("masters", "", "comma-separated master servers (deprecated, use -master instead)")
	a.dataDir = cmdAdmin.Flag.String("dataDir", "", "directory to store admin configuration and data files")

	a.adminUser = cmdAdmin.Flag.String("adminUser", "admin", "admin interface username")
	a.adminPassword = cmdAdmin.Flag.String("adminPassword", "", "admin interface password (if empty, auth is disabled)")
	a.readOnlyUser = cmdAdmin.Flag.String("readOnlyUser", "", "read-only user username (optional, for view-only access)")
	a.readOnlyPassword = cmdAdmin.Flag.String("readOnlyPassword", "", "read-only user password (optional, for view-only access; requires adminPassword to be set)")
	a.icebergPort = cmdAdmin.Flag.Int("iceberg.port", 8181, "Iceberg REST Catalog port (0 to hide in UI)")
	a.urlPrefix = cmdAdmin.Flag.String("urlPrefix", "", "URL path prefix when running behind a reverse proxy under a subdirectory (e.g. /seaweedfs)")
	a.debug = cmdAdmin.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	a.debugPort = cmdAdmin.Flag.Int("debug.port", 6060, "http port for debugging")
	a.cpuProfile = cmdAdmin.Flag.String("cpuprofile", "", "cpu profile output file")
	a.memProfile = cmdAdmin.Flag.String("memprofile", "", "memory profile output file")
}

var cmdAdmin = &Command{
	UsageLine: "admin -port=23646 -master=localhost:9333 [-port.grpc=33646] [-dataDir=/path/to/data]",
	Short:     "start SeaweedFS web admin interface",
	Long: `Start a web admin interface for SeaweedFS cluster management.

  The admin interface provides a modern web interface for:
  - Cluster topology visualization and monitoring
  - Volume management and operations
  - File browser and management
  - System metrics and performance monitoring
  - Configuration management
  - Maintenance operations

  The admin interface automatically discovers filers from the master servers.
  A gRPC server for worker connections runs on the configured gRPC port (default: HTTP port + 10000).

  Example Usage:
    weed admin -port=23646 -master="master1:9333,master2:9333"
    weed admin -port=23646 -master="localhost:9333" -dataDir="/var/lib/seaweedfs-admin"
    weed admin -port=23646 -port.grpc=33646 -master="localhost:9333" -dataDir="~/seaweedfs-admin"
    weed admin -port=9900 -port.grpc=19900 -master="localhost:9333"
    weed admin -port=23646 -master="localhost:9333" -urlPrefix="/seaweedfs"

  Data Directory:
    - If dataDir is specified, admin configuration and maintenance data is persisted
    - The directory will be created if it doesn't exist
    - Configuration files are stored in JSON format for easy editing
    - Without dataDir, all configuration is kept in memory only

  Authentication:
    - If adminPassword is not set, the admin interface runs without authentication
    - If adminPassword is set, users must login with adminUser/adminPassword (full access)
    - Optional read-only access: set readOnlyUser and readOnlyPassword for view-only access
    - Read-only users can view cluster status and configurations but cannot make changes
    - IMPORTANT: When read-only credentials are configured, adminPassword MUST also be set
    - This ensures an admin account exists to manage and authorize read-only access
    - Sessions are secured with auto-generated session keys
    - Credentials can also be set via security.toml [admin] section or environment variables:
      WEED_ADMIN_USER, WEED_ADMIN_PASSWORD, WEED_ADMIN_READONLY_USER, WEED_ADMIN_READONLY_PASSWORD
    - Precedence: CLI flag > env var / security.toml > default value

  Security Configuration:
    - The admin server reads TLS configuration from security.toml
    - Configure [https.admin] section in security.toml for HTTPS support
    - If https.admin.key is set, the server will start in TLS mode
    - If https.admin.ca is set, mutual TLS authentication is enabled
    - Set strong adminPassword for production deployments
    - Configure firewall rules to restrict admin interface access

  security.toml Example:
    [https.admin]
    cert = "/etc/ssl/admin.crt"
    key = "/etc/ssl/admin.key"
    ca = "/etc/ssl/ca.crt"     # optional, for mutual TLS

  Worker Communication:
    - Workers connect via gRPC on HTTP port + 10000
    - Workers use [grpc.admin] configuration from security.toml
    - TLS is automatically used if certificates are configured
    - Workers fall back to insecure connections if TLS is unavailable

  Plugin:
    - Always enabled on the worker gRPC port
    - Registers plugin.proto gRPC service on the same worker gRPC port
    - External workers connect with: weed worker -admin=<admin_host:admin_port>
    - Persists plugin metadata under dataDir/plugin when dataDir is configured

  URL Prefix (Subdirectory Deployment):
    - Use -urlPrefix to run the admin UI behind a reverse proxy under a subdirectory
    - Example: -urlPrefix="/seaweedfs" makes the UI available at /seaweedfs/admin
    - The reverse proxy should forward /seaweedfs/* requests to the admin server
    - All static assets, API endpoints, and navigation links will use the prefix
    - Session cookies are scoped to the prefix path

  Debugging and Profiling:
    - Use -debug to start a pprof HTTP server for live profiling (localhost only)
    - Set -debug.port to choose the pprof port (default 6060)
    - Profiles are accessible at http://127.0.0.1:<debug.port>/debug/pprof/
    - Use -cpuprofile and -memprofile to write profiles to files on shutdown
    - WARNING: -debug exposes runtime internals; use only in trusted environments
    - Examples:
      weed admin -debug -debug.port=6060 -master="localhost:9333"
      weed admin -cpuprofile=cpu.prof -memprofile=mem.prof -master="localhost:9333"

  Configuration File:
    - The security.toml file is read from ".", "$HOME/.seaweedfs/",
      "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order
    - Generate example security.toml: weed scaffold -config=security

`,
}

func runAdmin(cmd *Command, args []string) bool {
	if *a.debug {
		grace.StartDebugServer(*a.debugPort)
	}

	*a.cpuProfile = util.ResolvePath(*a.cpuProfile)
	*a.memProfile = util.ResolvePath(*a.memProfile)
	grace.SetupProfiling(*a.cpuProfile, *a.memProfile)

	// Load security configuration
	util.LoadSecurityConfiguration()

	// Apply security.toml / env var fallbacks for credential flags.
	// CLI flags take precedence over security.toml / WEED_* env vars.
	applyViperFallback(cmd, a.adminUser, "adminUser", "admin.user")
	applyViperFallback(cmd, a.adminPassword, "adminPassword", "admin.password")
	applyViperFallback(cmd, a.readOnlyUser, "readOnlyUser", "admin.readonly.user")
	applyViperFallback(cmd, a.readOnlyPassword, "readOnlyPassword", "admin.readonly.password")

	// Backward compatibility: if -masters is provided, use it
	if *a.masters != "" {
		*a.master = *a.masters
	}

	// Validate required parameters
	if *a.master == "" {
		fmt.Println("Error: master parameter is required")
		fmt.Println("Usage: weed admin -master=master1:9333,master2:9333")
		return false
	}

	// Validate that master string can be parsed
	masterAddresses := pb.ServerAddresses(*a.master).ToAddresses()
	if len(masterAddresses) == 0 {
		fmt.Println("Error: no valid master addresses found")
		fmt.Println("Usage: weed admin -master=master1:9333,master2:9333")
		return false
	}

	// Security validation: prevent empty username when password is set
	if *a.adminPassword != "" && *a.adminUser == "" {
		fmt.Println("Error: -adminUser cannot be empty when -adminPassword is set")
		return false
	}
	if *a.readOnlyPassword != "" && *a.readOnlyUser == "" {
		fmt.Println("Error: -readOnlyUser is required when -readOnlyPassword is set")
		return false
	}
	// Security validation: prevent username conflicts between admin and read-only users
	if *a.adminUser != "" && *a.readOnlyUser != "" && *a.adminUser == *a.readOnlyUser {
		fmt.Println("Error: -adminUser and -readOnlyUser must be different when both are configured")
		return false
	}
	// Security validation: admin password is required for read-only user
	if *a.readOnlyPassword != "" && *a.adminPassword == "" {
		fmt.Println("Error: -adminPassword must be set when -readOnlyPassword is configured")
		return false
	}

	// Set default gRPC port if not specified
	if *a.grpcPort == 0 {
		*a.grpcPort = *a.port + 10000
	}

	// Security warnings
	if *a.adminPassword == "" {
		fmt.Println("WARNING: Admin interface is running without authentication!")
		fmt.Println("         Set -adminPassword for production use")
	}

	fmt.Printf("Starting SeaweedFS Admin Interface on port %d\n", *a.port)
	fmt.Printf("Worker gRPC server will run on port %d\n", *a.grpcPort)
	fmt.Printf("Masters: %s\n", *a.master)
	fmt.Printf("Filers will be discovered automatically from masters\n")
	if *a.dataDir != "" {
		fmt.Printf("Data Directory: %s\n", *a.dataDir)
	} else {
		fmt.Printf("Data Directory: Not specified (configuration will be in-memory only)\n")
	}
	if *a.adminPassword != "" {
		fmt.Printf("Authentication: Enabled (admin user: %s)\n", *a.adminUser)
		if *a.readOnlyPassword != "" {
			fmt.Printf("Read-only access: Enabled (read-only user: %s)\n", *a.readOnlyUser)
		}
	} else {
		fmt.Printf("Authentication: Disabled\n")
	}
	fmt.Printf("Plugin: Enabled\n")

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal %v, shutting down gracefully...\n", sig)
		cancel()
	}()

	// Normalize URL prefix
	urlPrefix := strings.TrimRight(*a.urlPrefix, "/")
	if urlPrefix != "" && !strings.HasPrefix(urlPrefix, "/") {
		urlPrefix = "/" + urlPrefix
	}
	if urlPrefix != "" {
		fmt.Printf("URL Prefix: %s\n", urlPrefix)
	}

	// Start the admin server with all masters (UI enabled by default)
	err := startAdminServer(ctx, a, true, *a.icebergPort, urlPrefix)
	if err != nil {
		fmt.Printf("Admin server error: %v\n", err)
		return false
	}

	fmt.Println("Admin server stopped")
	return true
}

// startAdminServer starts the actual admin server
func startAdminServer(ctx context.Context, options AdminOptions, enableUI bool, icebergPort int, urlPrefix string) error {
	// Create router
	r := mux.NewRouter()
	r.Use(loggingMiddleware)
	r.Use(recoveryMiddleware)

	// Inject URL prefix into request context for use by handlers and templates
	if urlPrefix != "" {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := dash.WithURLPrefix(r.Context(), urlPrefix)
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		})
	}

	// Create data directory first if specified (needed for session key storage)
	var dataDir string
	if *options.dataDir != "" {
		dataDir = util.ResolvePath(*options.dataDir)
		if dataDir != *options.dataDir {
			fmt.Printf("Expanded dataDir: %s -> %s\n", *options.dataDir, dataDir)
		}
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory %s: %v", dataDir, err)
		}
		fmt.Printf("Data directory created/verified: %s\n", dataDir)
	}

	// Detect TLS configuration to set Secure cookie flag
	cookieSecure := viper.GetString("https.admin.key") != ""

	// Session store - load or generate session keys
	authKey, encKey, err := loadOrGenerateSessionKeys(dataDir)
	if err != nil {
		return fmt.Errorf("failed to get session key: %w", err)
	}
	store := sessions.NewCookieStore(authKey, encKey)

	// Configure session options to ensure cookies are properly saved
	cookiePath := "/"
	if urlPrefix != "" {
		cookiePath = urlPrefix + "/"
	}
	store.Options = &sessions.Options{
		Path:     cookiePath,
		MaxAge:   3600 * 24,    // 24 hours
		HttpOnly: true,         // Prevent JavaScript access
		Secure:   cookieSecure, // Set based on actual TLS configuration
		SameSite: http.SameSiteLaxMode,
	}

	// Static files - serve from embedded filesystem
	staticFS, err := admin.GetStaticFS()
	if err != nil {
		log.Printf("Warning: Failed to load embedded static files: %v", err)
	} else {
		staticHandler := http.FileServer(http.FS(staticFS))
		r.Handle("/static", http.RedirectHandler("/static/", http.StatusMovedPermanently))
		r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", staticHandler))
	}

	// Create admin server (plugin is always enabled)
	adminServer := dash.NewAdminServer(*options.master, nil, dataDir, icebergPort)

	// Show discovered filers
	filers := adminServer.GetAllFilers()
	if len(filers) > 0 {
		fmt.Printf("Discovered filers: %s\n", strings.Join(filers, ", "))
	} else {
		fmt.Printf("No filers discovered from masters\n")
	}

	// Start worker gRPC server for worker connections
	err = adminServer.StartWorkerGrpcServer(*options.grpcPort)
	if err != nil {
		return fmt.Errorf("failed to start worker gRPC server: %w", err)
	}

	// Set up cleanup for gRPC server
	defer func() {
		if stopErr := adminServer.StopWorkerGrpcServer(); stopErr != nil {
			log.Printf("Error stopping worker gRPC server: %v", stopErr)
		}
	}()

	// Create handlers and setup routes
	authRequired := *options.adminPassword != ""
	adminHandlers := handlers.NewAdminHandlers(adminServer, store)
	adminHandlers.SetupRoutes(r, authRequired, *options.adminUser, *options.adminPassword, *options.readOnlyUser, *options.readOnlyPassword, enableUI)

	// Server configuration
	addr := fmt.Sprintf(":%d", *options.port)
	var handler http.Handler = r
	if urlPrefix != "" {
		stripped := http.StripPrefix(urlPrefix, r)
		handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Redirect /prefix (no trailing slash) to /prefix/
			if req.URL.Path == urlPrefix {
				target := urlPrefix + "/"
				if req.URL.RawQuery != "" {
					target += "?" + req.URL.RawQuery
				}
				http.Redirect(w, req, target, http.StatusFound)
				return
			}
			stripped.ServeHTTP(w, req)
		})
	}
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	// Decide TLS configuration BEFORE launching the server goroutine, so a
	// bad cert or a missing key surfaces as a startup error instead of a
	// silently returned goroutine that leaves startAdminServer blocked on
	// ctx.Done() with no listener.
	var (
		clientCertFile,
		certFile,
		keyFile string
	)
	useTLS := false
	useMTLS := false

	if viper.GetString("https.admin.key") != "" {
		useTLS = true
		certFile = viper.GetString("https.admin.cert")
		keyFile = viper.GetString("https.admin.key")
	}

	if viper.GetString("https.admin.ca") != "" {
		useMTLS = true
		clientCertFile = viper.GetString("https.admin.ca")
	}

	if useMTLS {
		server.TLSConfig = security.LoadClientTLSHTTP(clientCertFile)
	}

	if useTLS {
		getCert, certProvider, certErr := security.NewReloadingServerCertificate(certFile, keyFile)
		if certErr != nil {
			return fmt.Errorf("load admin HTTPS certificate: %w", certErr)
		}
		defer certProvider.Close()
		if server.TLSConfig == nil {
			server.TLSConfig = &tls.Config{}
		}
		server.TLSConfig.GetCertificate = getCert
	}

	// Start server
	go func() {
		log.Printf("Starting SeaweedFS Admin Server on port %d", *options.port)
		var serveErr error
		if useTLS {
			log.Printf("Starting SeaweedFS Admin Server with TLS on port %d", *options.port)
			serveErr = server.ListenAndServeTLS("", "")
		} else {
			serveErr = server.ListenAndServe()
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.Printf("Failed to start server: %v", serveErr)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	log.Println("Shutting down admin server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("admin server forced to shutdown: %w", err)
	}

	adminServer.Shutdown()

	return nil
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (r *statusRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := r.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}

		next.ServeHTTP(recorder, r)

		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		if status >= 200 && status < 300 {
			return
		}

		log.Printf("[HTTP] %v | %3d | %13v | %15s | %-7s %s",
			time.Now().Format("2006/01/02 - 15:04:05"),
			status,
			time.Since(start),
			r.RemoteAddr,
			r.Method,
			r.URL.Path,
		)
	})
}

func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("panic: %v\n%s", err, debug.Stack())
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// loadOrGenerateSessionKeys loads or creates authentication/encryption keys for session cookies.
func loadOrGenerateSessionKeys(dataDir string) ([]byte, []byte, error) {
	const keyLen = 32

	if dataDir == "" {
		// No persistence, generate ephemeral keys
		log.Println("No dataDir specified, generating ephemeral session keys")
		authKey := make([]byte, keyLen)
		encKey := make([]byte, keyLen)
		if _, err := rand.Read(authKey); err != nil {
			return nil, nil, err
		}
		if _, err := rand.Read(encKey); err != nil {
			return nil, nil, err
		}
		return authKey, encKey, nil
	}

	sessionKeyPath := filepath.Join(dataDir, ".session_key")

	if data, err := os.ReadFile(sessionKeyPath); err == nil {
		switch len(data) {
		case keyLen:
			authKey := make([]byte, keyLen)
			copy(authKey, data)

			encKey := make([]byte, keyLen)
			if _, err := rand.Read(encKey); err != nil {
				return nil, nil, err
			}
			log.Printf("Warning: Upgrading session key at %s by adding an encryption key; existing cookies will be invalidated", sessionKeyPath)

			combined := append(authKey, encKey...)
			if err := os.WriteFile(sessionKeyPath, combined, 0600); err != nil {
				log.Printf("Warning: Failed to persist upgraded session key: %v", err)
			} else {
				log.Printf("Upgraded session key file to include encryption key: %s", sessionKeyPath)
			}
			return authKey, encKey, nil
		case 2 * keyLen:
			authKey := make([]byte, keyLen)
			encKey := make([]byte, keyLen)
			copy(authKey, data[:keyLen])
			copy(encKey, data[keyLen:])
			log.Printf("Loaded persisted session key from %s", sessionKeyPath)
			return authKey, encKey, nil
		default:
			log.Printf("Warning: Invalid session key file (expected %d or %d bytes, got %d), generating new key", keyLen, 2*keyLen, len(data))
		}
	} else if !os.IsNotExist(err) {
		log.Printf("Warning: Failed to read session key from %s: %v. A new key will be generated.", sessionKeyPath, err)
	}

	key := make([]byte, 2*keyLen)
	if _, err := rand.Read(key); err != nil {
		return nil, nil, err
	}

	if err := os.WriteFile(sessionKeyPath, key, 0600); err != nil {
		log.Printf("Warning: Failed to persist session key: %v", err)
	} else {
		log.Printf("Generated and persisted new session key to %s", sessionKeyPath)
	}

	return key[:keyLen], key[keyLen:], nil
}

// applyViperFallback sets a flag's value from viper (security.toml / env var)
// when the flag was not explicitly set on the command line.
func applyViperFallback(cmd *Command, flagPtr *string, flagName, viperKey string) {
	explicitlySet := false
	cmd.Flag.Visit(func(f *flag.Flag) {
		if f.Name == flagName {
			explicitlySet = true
		}
	})
	if !explicitlySet {
		if v := util.GetViper().GetString(viperKey); v != "" {
			*flagPtr = v
		}
	}
}
