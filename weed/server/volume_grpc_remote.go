package weed_server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	azureremote "github.com/seaweedfs/seaweedfs/weed/remote_storage/azure"
	s3remote "github.com/seaweedfs/seaweedfs/weed/remote_storage/s3"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// lookupIPAddrFunc resolves a host to one or more IP addresses. It is a
// package-level variable so tests can substitute a deterministic resolver.
var lookupIPAddrFunc = net.DefaultResolver.LookupIPAddr

// blockedIMDSHosts lists hostnames that target cloud instance metadata
// services (IMDS). These are blocked regardless of how they happen to
// resolve, because some environments alias the IMDS address under a name.
var blockedIMDSHosts = map[string]struct{}{
	"metadata.google.internal": {},
	"metadata":                 {},
}

// validateRemoteEndpoint returns an error if the supplied S3 endpoint is not
// safe to dial from a server that has network access to cluster-internal
// hosts. It rejects empty/non-http(s) schemes, loopback/link-local/
// unspecified addresses, RFC 1918 + CGNAT ranges, and well-known IMDS
// hostnames. Operators that legitimately fetch from private hosts can opt
// out with -volume.allowUntrustedRemoteEndpoints.
func validateRemoteEndpoint(ctx context.Context, endpoint string) error {
	if strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("remote endpoint is empty")
	}
	u, parseErr := url.Parse(endpoint)
	if parseErr != nil {
		return fmt.Errorf("parse remote endpoint %q: %w", endpoint, parseErr)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("remote endpoint %q must use http or https, got %q", endpoint, u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("remote endpoint %q has no host", endpoint)
	}
	lowerHost := strings.ToLower(host)
	if _, ok := blockedIMDSHosts[lowerHost]; ok {
		return fmt.Errorf("remote endpoint %q targets instance metadata service", endpoint)
	}
	if ip := net.ParseIP(host); ip != nil {
		if err := checkBlockedIP(endpoint, ip); err != nil {
			return err
		}
		return nil
	}
	resolveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	addrs, lookupErr := lookupIPAddrFunc(resolveCtx, host)
	if lookupErr != nil {
		return fmt.Errorf("resolve remote endpoint host %q: %w", host, lookupErr)
	}
	for _, addr := range addrs {
		if err := checkBlockedIP(endpoint, addr.IP); err != nil {
			return err
		}
	}
	return nil
}

// imdsIPv4 is the AWS/Azure/GCP IPv4 IMDS address. It is link-local and is
// already covered by IsLinkLocalUnicast, but is named explicitly so the
// error message is unambiguous in logs.
var imdsIPv4 = net.ParseIP("169.254.169.254")

// cgnatNet is the RFC 6598 carrier-grade NAT range (100.64.0.0/10). The
// stdlib's IsPrivate covers RFC 1918 but not CGNAT, so check it explicitly.
var cgnatNet = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

func checkBlockedIP(endpoint string, ip net.IP) error {
	return checkBlockedIPPolicy(endpoint, ip, false)
}

// checkBlockedIPPolicy rejects addresses that must never be dialed from a
// server with cluster-internal reach. allowPrivate keeps RFC 1918 / CGNAT
// reachable for callers whose target legitimately sits on an internal network
// (peer volume servers), while still blocking loopback, link-local (IMDS) and
// unspecified.
func checkBlockedIPPolicy(endpoint string, ip net.IP, allowPrivate bool) error {
	if ip == nil {
		return nil
	}
	if ip.Equal(imdsIPv4) {
		return fmt.Errorf("remote endpoint %q targets instance metadata service %s", endpoint, ip)
	}
	switch {
	case ip.IsLoopback():
		return fmt.Errorf("remote endpoint %q resolves to loopback address %s", endpoint, ip)
	case ip.IsUnspecified():
		return fmt.Errorf("remote endpoint %q resolves to unspecified address %s", endpoint, ip)
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		return fmt.Errorf("remote endpoint %q resolves to link-local address %s", endpoint, ip)
	case ip.IsInterfaceLocalMulticast():
		return fmt.Errorf("remote endpoint %q resolves to interface-local address %s", endpoint, ip)
	}
	if !allowPrivate {
		switch {
		case ip.IsPrivate():
			return fmt.Errorf("remote endpoint %q resolves to private address %s", endpoint, ip)
		case cgnatNet.Contains(ip):
			return fmt.Errorf("remote endpoint %q resolves to CGNAT address %s", endpoint, ip)
		}
	}
	// IPv6 transition addresses embed an IPv4 destination that routes to the
	// same host wherever the matching relay exists (common in IPv6-only cloud).
	// net.IP only normalizes ::ffff: mapped addresses, so pull the embedded
	// IPv4 out of the other forms and re-check it against the deny list.
	if embedded := embeddedTransitionIPv4(ip); embedded != nil {
		return checkBlockedIPPolicy(endpoint, embedded, allowPrivate)
	}
	return nil
}

// validateReplicaTarget rejects a replica upload target that could redirect the
// forwarded write away from a peer volume server. The target must be a bare
// host:port -- a scheme, userinfo, path, query or fragment can smuggle a
// different destination through fmt.Sprintf -- whose host is not loopback,
// link-local (IMDS) or unspecified. Cluster peers legitimately sit on private
// networks, so RFC 1918 / CGNAT are allowed.
func validateReplicaTarget(ctx context.Context, target string) error {
	if strings.TrimSpace(target) == "" {
		return fmt.Errorf("replica target is empty")
	}
	if strings.Contains(target, "://") || strings.ContainsAny(target, "/?#@\\") {
		return fmt.Errorf("replica target %q must be a bare host:port", target)
	}
	host, _, splitErr := net.SplitHostPort(target)
	if splitErr != nil {
		return fmt.Errorf("replica target %q must be a bare host:port: %w", target, splitErr)
	}
	if host == "" {
		return fmt.Errorf("replica target %q has no host", target)
	}
	if _, ok := blockedIMDSHosts[strings.ToLower(host)]; ok {
		return fmt.Errorf("replica target %q targets instance metadata service", target)
	}
	if ip := net.ParseIP(host); ip != nil {
		return checkBlockedIPPolicy(target, ip, true)
	}
	resolveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	addrs, lookupErr := lookupIPAddrFunc(resolveCtx, host)
	if lookupErr != nil {
		return fmt.Errorf("resolve replica target host %q: %w", host, lookupErr)
	}
	for _, addr := range addrs {
		if err := checkBlockedIPPolicy(target, addr.IP, true); err != nil {
			return err
		}
	}
	return nil
}

// embeddedTransitionIPv4 returns the IPv4 address carried by an IPv6 transition
// address -- NAT64 64:ff9b::/96 (RFC 6052), 6to4 2002::/16 (RFC 3056), Teredo
// 2001:0000::/32 (RFC 4380), and the deprecated IPv4-compatible ::/96 (RFC
// 4291) -- or nil when ip is not one of those. IPv4-mapped ::ffff:0:0/96 is
// excluded because net.IP already normalizes it via To4.
func embeddedTransitionIPv4(ip net.IP) net.IP {
	v6 := ip.To16()
	if v6 == nil || ip.To4() != nil {
		return nil
	}
	switch {
	case v6[0] == 0x00 && v6[1] == 0x64 && v6[2] == 0xff && v6[3] == 0x9b && allZero(v6[4:12]):
		return net.IPv4(v6[12], v6[13], v6[14], v6[15])
	case v6[0] == 0x20 && v6[1] == 0x02:
		return net.IPv4(v6[2], v6[3], v6[4], v6[5])
	case v6[0] == 0x20 && v6[1] == 0x01 && v6[2] == 0x00 && v6[3] == 0x00:
		// Teredo obfuscates the client IPv4 as its ones' complement.
		return net.IPv4(v6[12]^0xff, v6[13]^0xff, v6[14]^0xff, v6[15]^0xff)
	case allZero(v6[:12]):
		// IPv4-compatible ::a.b.c.d; :: and ::1 are already handled above.
		return net.IPv4(v6[12], v6[13], v6[14], v6[15])
	}
	return nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// guardedDialer returns a DialContext that resolves the host itself and
// re-applies checkBlockedIP to every resolved address immediately before
// dialing. This closes the DNS-rebinding window between
// validateRemoteEndpoint and the actual TCP connect performed by the AWS S3
// client: even if the attacker's DNS flips to 127.0.0.1 (or any other
// blocked range) after the up-front check, the dial is refused.
func guardedDialer(endpoint string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return guardedDialerPolicy(endpoint, false)
}

// guardedDialerPolicy is guardedDialer with the same allowPrivate knob as
// checkBlockedIPPolicy, so the replica upload path can keep dialing private
// peers while still refusing loopback / link-local / unspecified at connect
// time (closing the rebinding window for replica hostnames too).
func guardedDialerPolicy(endpoint string, allowPrivate bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return nil, splitErr
		}
		// If the host is already a literal IP just validate and dial it.
		if ip := net.ParseIP(host); ip != nil {
			if err := checkBlockedIPPolicy(endpoint, ip, allowPrivate); err != nil {
				return nil, err
			}
			return dialer.DialContext(ctx, network, addr)
		}
		// Otherwise resolve, validate every answer, and dial the first IP
		// that passes the deny list. Using a literal-IP target prevents the
		// kernel resolver in net.Dialer from looking the name up a second
		// time inside Dial and getting a different answer.
		addrs, lookupErr := lookupIPAddrFunc(ctx, host)
		if lookupErr != nil {
			return nil, fmt.Errorf("resolve remote endpoint host %q: %w", host, lookupErr)
		}
		var firstBlockErr error
		for _, a := range addrs {
			if err := checkBlockedIPPolicy(endpoint, a.IP, allowPrivate); err != nil {
				if firstBlockErr == nil {
					firstBlockErr = err
				}
				continue
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(a.IP.String(), port))
		}
		if firstBlockErr != nil {
			return nil, firstBlockErr
		}
		return nil, fmt.Errorf("resolve remote endpoint host %q: no addresses", host)
	}
}

// newGuardedHTTPClient returns an *http.Client whose transport refuses to
// dial addresses that fail checkBlockedIP at connect time. It is meant for
// per-request use; do not share across remote configs.
func newGuardedHTTPClient(endpoint string) *http.Client {
	return newGuardedHTTPClientPolicy(endpoint, false)
}

// newGuardedHTTPClientPolicy is newGuardedHTTPClient with the allowPrivate knob
// for the replica upload path, whose targets are cluster peers on private
// networks.
func newGuardedHTTPClientPolicy(endpoint string, allowPrivate bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			// No proxy: guardedDialer must see the real target address. Through
			// a proxy it would only validate the proxy's IP while the proxy
			// re-resolves the endpoint host, reopening the rebinding window the
			// dialer exists to close. Operators that need a proxy can opt out
			// with -volume.allowUntrustedRemoteEndpoints.
			Proxy:                 nil,
			DialContext:           guardedDialerPolicy(endpoint, allowPrivate),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          16,
			IdleConnTimeout:       60 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

// guardedRemoteClient reports the caller-supplied endpoint a backend dials
// directly and a constructor that routes through the given HTTP client, or
// ok=false for backends that only reach a fixed provider host. The S3-SDK
// family and azure (once AzureEndpoint is set) both honor an attacker-supplied
// endpoint, so both must pass the SSRF deny-list and rebinding-safe dialer.
func guardedRemoteClient(remoteConf *remote_pb.RemoteConf) (endpoint string, makeClient func(*http.Client) (remote_storage.RemoteStorageClient, error), ok bool) {
	if remoteConf == nil {
		return "", nil, false
	}
	if ep, isS3 := s3remote.S3CompatibleEndpoint(remoteConf); isS3 {
		return ep, func(httpClient *http.Client) (remote_storage.RemoteStorageClient, error) {
			return s3remote.MakeWithHTTPClient(remoteConf, httpClient)
		}, true
	}
	if remoteConf.Type == "azure" && remoteConf.AzureEndpoint != "" {
		return remoteConf.AzureEndpoint, func(httpClient *http.Client) (remote_storage.RemoteStorageClient, error) {
			return azureremote.MakeWithHTTPClient(remoteConf, httpClient)
		}, true
	}
	return "", nil, false
}

// gcsCredentialsArePath reports whether a gcs credentials value is a filesystem
// path rather than inline JSON, matching the gcs client's own inline detection.
func gcsCredentialsArePath(creds string) bool {
	return creds != "" && !strings.HasPrefix(creds, "{")
}

func (vs *VolumeServer) FetchAndWriteNeedle(ctx context.Context, req *volume_server_pb.FetchAndWriteNeedleRequest) (resp *volume_server_pb.FetchAndWriteNeedleResponse, err error) {
	if err := vs.checkGrpcAdminAuth(ctx); err != nil {
		return nil, err
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	resp = &volume_server_pb.FetchAndWriteNeedleResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	remoteConf := req.RemoteConf

	if !vs.AllowUntrustedRemoteEndpoints && remoteConf != nil {
		// A gcs credentials value that is a filesystem path is read from disk by
		// the SDK. Accept only inline JSON on the request; the server env var
		// still supplies a path.
		if gcsCredentialsArePath(remoteConf.GetGcsGoogleApplicationCredentials()) {
			return nil, fmt.Errorf("reject remote credentials: gcs credentials must be inline JSON")
		}
	}

	var client remote_storage.RemoteStorageClient
	var getClientErr error
	if endpoint, makeClient, ok := guardedRemoteClient(remoteConf); ok && !vs.AllowUntrustedRemoteEndpoints {
		if validateErr := validateRemoteEndpoint(ctx, endpoint); validateErr != nil {
			return nil, fmt.Errorf("reject remote endpoint: %w", validateErr)
		}
		// Build a one-shot client whose dial path re-validates the resolved
		// IP every time. This pins the validated endpoint against DNS
		// rebinding (a hostname that resolves to a public IP for
		// validateRemoteEndpoint and then flips to 127.0.0.1 / 169.254.x.x
		// when the SDK dials).
		client, getClientErr = makeClient(newGuardedHTTPClient(endpoint))
	} else {
		client, getClientErr = remote_storage.GetRemoteStorage(remoteConf)
	}
	if getClientErr != nil {
		return nil, fmt.Errorf("get remote client: %w", getClientErr)
	}

	remoteStorageLocation := req.RemoteLocation

	var data []byte
	var readRemoteErr error
	if cr, ok := client.(remote_storage.RemoteStorageConcurrentReader); ok {
		concurrency := int(req.DownloadConcurrency)
		if concurrency <= 0 {
			concurrency = 0 // let the implementation choose its default
		} else if concurrency > 64 {
			concurrency = 64
		}
		data, readRemoteErr = cr.ReadFileWithConcurrency(remoteStorageLocation, req.Offset, req.Size, concurrency)
	} else {
		data, readRemoteErr = client.ReadFile(remoteStorageLocation, req.Offset, req.Size)
	}
	if readRemoteErr != nil {
		return nil, fmt.Errorf("read from remote %+v: %w", remoteStorageLocation, readRemoteErr)
	}
	// The chunk is recorded with the requested size, so a short read would be
	// cached as a full-size chunk with a zero-padded or truncated tail. Fail
	// loudly instead of persisting silently corrupt content.
	if int64(len(data)) != req.Size {
		return nil, fmt.Errorf("read from remote %+v: got %d bytes, want %d", remoteStorageLocation, len(data), req.Size)
	}

	// Validate every replica target before writing anything, so a malformed or
	// internal target fails the request instead of leaving a local write behind.
	if !vs.AllowUntrustedRemoteEndpoints {
		for _, replica := range req.Replicas {
			if validateErr := validateReplicaTarget(ctx, replica.Url); validateErr != nil {
				return nil, fmt.Errorf("reject replica target: %w", validateErr)
			}
		}
	}

	var wg sync.WaitGroup
	var localErr error
	replicaErrs := make([]error, len(req.Replicas))
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := new(needle.Needle)
		n.Id = types.NeedleId(req.NeedleId)
		n.Cookie = types.Cookie(req.Cookie)
		n.Data, n.DataSize = data, uint32(len(data))
		// copied from *Needle.prepareWriteBuffer()
		n.Size = 4 + types.Size(n.DataSize) + 1
		n.Checksum = needle.NewCRC(n.Data)
		n.LastModified = uint64(time.Now().Unix())
		n.SetHasLastModifiedDate()
		if _, localWriteErr := vs.store.WriteVolumeNeedle(v.Id, n, true, false); localWriteErr != nil {
			localErr = fmt.Errorf("local write needle %d size %d: %v", req.NeedleId, req.Size, localWriteErr)
		} else {
			resp.ETag = n.Etag()
		}
	}()
	if len(req.Replicas) > 0 {
		fileId := needle.NewFileId(v.Id, req.NeedleId, req.Cookie)
		for i, replica := range req.Replicas {
			wg.Add(1)
			go func(idx int, targetVolumeServer string) {
				defer wg.Done()
				uploadOption := &operation.UploadOption{
					UploadUrl:         fmt.Sprintf("http://%s/%s?type=replicate", targetVolumeServer, fileId.String()),
					Filename:          "",
					Cipher:            false,
					IsInputCompressed: false,
					IsReplication:     true,
					MimeType:          "",
					PairMap:           nil,
					Jwt:               security.EncodedJwt(req.Auth),
				}

				// Upload through a client that re-checks the target at connect
				// time, so a replica hostname cannot rebind to a blocked address
				// after validateReplicaTarget. Peers may be private, so allow
				// private here; the opt-out uses the shared global client.
				var uploader *operation.Uploader
				if vs.AllowUntrustedRemoteEndpoints {
					var uploaderErr error
					uploader, uploaderErr = operation.NewUploader()
					if uploaderErr != nil {
						replicaErrs[idx] = fmt.Errorf("remote write needle %d size %d: %v", req.NeedleId, req.Size, uploaderErr)
						return
					}
				} else {
					uploader = operation.NewUploaderWithHttpClient(newGuardedHTTPClientPolicy(targetVolumeServer, true))
				}

				if _, replicaWriteErr := uploader.UploadData(ctx, data, uploadOption); replicaWriteErr != nil {
					replicaErrs[idx] = fmt.Errorf("remote write needle %d size %d: %v", req.NeedleId, req.Size, replicaWriteErr)
				}
			}(i, replica.Url)
		}
	}

	wg.Wait()

	// local write error wins; otherwise surface the first replica failure
	err = localErr
	for _, replicaErr := range replicaErrs {
		if err == nil {
			err = replicaErr
		}
	}

	return resp, err
}
