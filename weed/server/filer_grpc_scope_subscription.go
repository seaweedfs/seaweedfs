package weed_server

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Only the scope interceptor produces this marker, after verifying a general
// filer token. Dedicated subscription tokens keep their existing verifier.
type scopeSubscriptionContextKey struct{}

// checkSubscribe bounds every requested selector. Event filtering also enforces
// path-segment boundaries because the subscription's prefix match is textual.
func (sc *scope) checkSubscribe(method string, req *filer_pb.SubscribeMetadataRequest) error {
	req.ClientSupportsMetadataChunks = false
	kept := req.PathPrefixes[:0]
	for _, p := range req.PathPrefixes {
		// Mounts also watch the storage-rule file, which scoped reads hide.
		if p != scopeFilerConfDir+"/"+scopeFilerConfName {
			kept = append(kept, p)
		}
	}
	req.PathPrefixes = kept
	prefixes := append([]string{}, req.PathPrefixes...)
	if req.PathPrefix != "" {
		prefixes = append(prefixes, req.PathPrefix)
	}
	if len(prefixes) == 0 && len(req.Directories) == 0 {
		return sc.deny(method, "a subscription must name at least one path prefix or directory")
	}
	for _, p := range prefixes {
		// Mounts use a trailing slash to subscribe below their root.
		if len(p) > 1 {
			p = strings.TrimSuffix(p, "/")
		}
		if _, err := sc.checkPath(method, p, "", false); err != nil {
			return err
		}
	}
	for _, dir := range req.Directories {
		if _, err := sc.checkPath(method, dir, "", false); err != nil {
			return err
		}
	}
	return nil
}

// filterEvent checks the envelope's event as well as every batched event. A
// hidden head keeps only its position when visible tail events remain. Shared
// input messages are never modified, and log-file references never pass through.
func (sc *scope) filterEvent(resp *filer_pb.SubscribeMetadataResponse) *filer_pb.SubscribeMetadataResponse {
	if resp == nil {
		return nil
	}
	head := sc.filterSubscriptionEvent(resp)
	var kept []*filer_pb.SubscribeMetadataResponse
	for _, event := range resp.Events {
		if out := sc.filterEvent(event); out != nil {
			kept = append(kept, out)
		}
	}
	if head == nil {
		if len(kept) == 0 {
			return nil
		}
		head = &filer_pb.SubscribeMetadataResponse{
			TsNs: resp.TsNs, FlushedTsNs: resp.FlushedTsNs,
			EventNotification: &filer_pb.EventNotification{},
		}
	}
	head.Events = kept
	return head
}

// filterSubscriptionEvent exposes only the visible half of a boundary-crossing
// rename. A rename from filer.conf is entirely hidden: its new side would carry
// the hidden file's data. A rename into it may still delete the visible old side.
func (sc *scope) filterSubscriptionEvent(resp *filer_pb.SubscribeMetadataResponse) *filer_pb.SubscribeMetadataResponse {
	if len(resp.LogFileRefs) > 0 {
		return nil
	}
	out := &filer_pb.SubscribeMetadataResponse{TsNs: resp.TsNs, FlushedTsNs: resp.FlushedTsNs}
	ev := resp.EventNotification
	if ev == nil {
		return out
	}
	if ev.OldEntry == nil && ev.NewEntry == nil {
		if resp.Directory != "" {
			p, err := scopeFullPath(resp.Directory, "")
			if err != nil || !scopePathAllowed(sc.grants, p, false) || isScopeFilerConf(resp.Directory, "") {
				return nil
			}
			out.Directory = resp.Directory
		}
		out.EventNotification = &filer_pb.EventNotification{}
		return out
	}
	newDir := filer_pb.MetadataEventTargetDirectory(resp)
	visible := func(dir string, entry *filer_pb.Entry) (bool, error) {
		if entry == nil {
			return false, nil
		}
		if entry.Name == "" {
			return false, errScopePathNotCanonical
		}
		p, err := scopeFullPath(dir, entry.Name)
		return err == nil && scopePathAllowed(sc.grants, p, false) && !isScopeFilerConf(dir, entry.Name), err
	}
	oldOK, oldErr := visible(resp.Directory, ev.OldEntry)
	newOK, newErr := visible(newDir, ev.NewEntry)
	if oldErr != nil || newErr != nil ||
		(ev.OldEntry != nil && isScopeFilerConf(resp.Directory, ev.OldEntry.Name)) ||
		(!oldOK && !newOK) {
		return nil
	}
	out.Directory, out.MutationTsNs = resp.Directory, resp.MutationTsNs
	out.EventNotification = &filer_pb.EventNotification{
		DeleteChunks: ev.DeleteChunks, IsFromOtherCluster: ev.IsFromOtherCluster, Signatures: ev.Signatures,
	}
	if oldOK {
		out.EventNotification.OldEntry = ev.OldEntry
	}
	if newOK {
		out.EventNotification.NewEntry = ev.NewEntry
		out.EventNotification.NewParentPath = newDir
		if !oldOK {
			out.Directory = newDir
		}
	}
	return out
}
