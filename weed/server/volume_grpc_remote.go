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
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
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
	case ip.IsPrivate():
		return fmt.Errorf("remote endpoint %q resolves to private address %s", endpoint, ip)
	case cgnatNet.Contains(ip):
		return fmt.Errorf("remote endpoint %q resolves to CGNAT address %s", endpoint, ip)
	}
	return nil
}

// guardedDialer returns a DialContext that resolves the host itself and
// re-applies checkBlockedIP to every resolved address immediately before
// dialing. This closes the DNS-rebinding window between
// validateRemoteEndpoint and the actual TCP connect performed by the AWS S3
// client: even if the attacker's DNS flips to 127.0.0.1 (or any other
// blocked range) after the up-front check, the dial is refused.
func guardedDialer(endpoint string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return nil, splitErr
		}
		// If the host is already a literal IP just validate and dial it.
		if ip := net.ParseIP(host); ip != nil {
			if err := checkBlockedIP(endpoint, ip); err != nil {
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
			if err := checkBlockedIP(endpoint, a.IP); err != nil {
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
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           guardedDialer(endpoint),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          16,
			IdleConnTimeout:       60 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
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

	var client remote_storage.RemoteStorageClient
	var getClientErr error
	if !vs.AllowUntrustedRemoteEndpoints && remoteConf != nil && remoteConf.Type == "s3" {
		// Endpoint validation is S3-specific: only RemoteConf.S3Endpoint
		// is a URL the volume server dials directly. Other backends
		// (gcs, azure, ...) authenticate against their own SDKs and
		// don't accept an attacker-controlled host.
		if validateErr := validateRemoteEndpoint(ctx, remoteConf.S3Endpoint); validateErr != nil {
			return nil, fmt.Errorf("reject remote endpoint: %w", validateErr)
		}
		// Build a one-shot S3 client whose dial path re-validates the
		// resolved IP every time. This pins the validated endpoint against
		// DNS rebinding (a hostname that resolves to a public IP for
		// validateRemoteEndpoint and then flips to 127.0.0.1 / 169.254.x.x
		// when the AWS SDK dials).
		client, getClientErr = s3remote.MakeWithHTTPClient(remoteConf, newGuardedHTTPClient(remoteConf.S3Endpoint))
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

	var wg sync.WaitGroup
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
			if err == nil {
				err = fmt.Errorf("local write needle %d size %d: %v", req.NeedleId, req.Size, localWriteErr)
			}
		} else {
			resp.ETag = n.Etag()
		}
	}()
	if len(req.Replicas) > 0 {
		fileId := needle.NewFileId(v.Id, req.NeedleId, req.Cookie)
		for _, replica := range req.Replicas {
			wg.Add(1)
			go func(targetVolumeServer string) {
				defer wg.Done()
				uploadOption := &operation.UploadOption{
					UploadUrl:         fmt.Sprintf("http://%s/%s?type=replicate", targetVolumeServer, fileId.String()),
					Filename:          "",
					Cipher:            false,
					IsInputCompressed: false,
					MimeType:          "",
					PairMap:           nil,
					Jwt:               security.EncodedJwt(req.Auth),
				}

				uploader, uploaderErr := operation.NewUploader()
				if uploaderErr != nil && err == nil {
					err = fmt.Errorf("remote write needle %d size %d: %v", req.NeedleId, req.Size, uploaderErr)
					return
				}

				if _, replicaWriteErr := uploader.UploadData(ctx, data, uploadOption); replicaWriteErr != nil && err == nil {
					err = fmt.Errorf("remote write needle %d size %d: %v", req.NeedleId, req.Size, replicaWriteErr)
				}
			}(replica.Url)
		}
	}

	wg.Wait()

	return resp, err
}
