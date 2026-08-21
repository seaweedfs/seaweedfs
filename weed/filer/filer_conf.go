package filer

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/viant/ptrie"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	DirectoryEtcRoot      = "/etc/"
	DirectoryEtcSeaweedFS = "/etc/seaweedfs"
	DirectoryEtcRemote    = "/etc/remote"
	FilerConfName         = "filer.conf"
	IamConfigDirectory    = "/etc/iam"
	IamIdentityFile       = "identity.json"
	IamPoliciesFile       = "policies.json"
)

// FilerConfVersion is stamped into every configuration this build writes.
// Version 0 predates worm presence: it was written with EmitUnpopulated, so every
// rule carries an explicit "worm": false that meant nothing. Reading one back as an
// override would silently lift worm off nested paths, so it is dropped to unset.
const FilerConfVersion = 1

type FilerConf struct {
	rules ptrie.Trie[*filer_pb.FilerConf_PathConf]
}

func ReadFilerConf(filerGrpcAddress pb.ServerAddress, grpcDialOption grpc.DialOption, masterClient *wdclient.MasterClient) (*FilerConf, error) {
	return ReadFilerConfFromFilers([]pb.ServerAddress{filerGrpcAddress}, grpcDialOption, masterClient)
}

// ReadFilerConfFromFilers reads filer configuration with multi-filer failover support
func ReadFilerConfFromFilers(filerGrpcAddresses []pb.ServerAddress, grpcDialOption grpc.DialOption, masterClient *wdclient.MasterClient) (*FilerConf, error) {
	var data []byte
	if err := pb.WithOneOfGrpcFilerClients(false, filerGrpcAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if masterClient != nil {
			var buf bytes.Buffer
			if err := ReadEntry(masterClient, client, DirectoryEtcSeaweedFS, FilerConfName, &buf); err != nil {
				return err
			}
			data = buf.Bytes()
			return nil
		}
		content, err := ReadInsideFiler(context.Background(), client, DirectoryEtcSeaweedFS, FilerConfName)
		if err != nil {
			return err
		}
		data = content
		return nil
	}); err != nil && err != filer_pb.ErrNotFound {
		return nil, fmt.Errorf("read %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
	}

	fc := NewFilerConf()
	if len(data) > 0 {
		if err := fc.LoadFromBytes(data); err != nil {
			return nil, fmt.Errorf("parse %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
		}
	}
	return fc, nil
}

func NewFilerConf() (fc *FilerConf) {
	fc = &FilerConf{
		rules: ptrie.New[*filer_pb.FilerConf_PathConf](),
	}
	return fc
}

func (fc *FilerConf) loadFromFiler(filer *Filer) (err error) {
	filerConfPath := util.NewFullPath(DirectoryEtcSeaweedFS, FilerConfName)
	entry, err := filer.FindEntry(context.Background(), filerConfPath)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil
		}
		glog.Errorf("read filer conf entry %s: %v", filerConfPath, err)
		return
	}

	if len(entry.Content) > 0 {
		return fc.LoadFromBytes(entry.Content)
	}

	return fc.loadFromChunks(filer, entry.Content, entry.GetChunks(), entry.Size())
}

func (fc *FilerConf) loadFromChunks(filer *Filer, content []byte, chunks []*filer_pb.FileChunk, size uint64) (err error) {
	if len(content) == 0 {
		content, err = filer.readEntry(chunks, size)
		if err != nil {
			glog.Errorf("read filer conf content: %v", err)
			return
		}
	}

	return fc.LoadFromBytes(content)
}

func (fc *FilerConf) LoadFromBytes(data []byte) (err error) {
	conf := &filer_pb.FilerConf{}

	if err := jsonpb.Unmarshal(data, conf); err != nil {
		return err
	}

	return fc.doLoadConf(conf)
}

func (fc *FilerConf) doLoadConf(conf *filer_pb.FilerConf) (err error) {
	for _, location := range conf.Locations {
		if conf.Version < FilerConfVersion && location.Worm != nil && !*location.Worm {
			location.Worm = nil
		}
		err = fc.SetLocationConf(location)
		if err != nil {
			// this is not recoverable
			return nil
		}
	}
	return nil
}

func (fc *FilerConf) GetLocationConf(locationPrefix string) (locConf *filer_pb.FilerConf_PathConf, found bool) {
	return fc.rules.Get([]byte(locationPrefix))
}

func (fc *FilerConf) SetLocationConf(locConf *filer_pb.FilerConf_PathConf) (err error) {
	err = fc.rules.Put([]byte(locConf.LocationPrefix), locConf)
	if err != nil {
		glog.Errorf("put location prefix: %v", err)
	}
	return
}

func (fc *FilerConf) AddLocationConf(locConf *filer_pb.FilerConf_PathConf) (err error) {
	existingConf, found := fc.rules.Get([]byte(locConf.LocationPrefix))
	if found {
		mergePathConf(existingConf, locConf)
		locConf = existingConf
	}
	err = fc.rules.Put([]byte(locConf.LocationPrefix), locConf)
	if err != nil {
		glog.Errorf("put location prefix: %v", err)
	}
	return
}

func (fc *FilerConf) DeleteLocationConf(locationPrefix string) {
	rules := ptrie.New[*filer_pb.FilerConf_PathConf]()
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		if string(key) == locationPrefix {
			return true
		}
		key = bytes.Clone(key)
		_ = rules.Put(key, value)
		return true
	})
	fc.rules = rules
}

// emptyPathConf is a singleton for paths with no matching rules
// Callers must NOT mutate the returned value
var emptyPathConf = &filer_pb.FilerConf_PathConf{}

func (fc *FilerConf) MatchStorageRule(path string) (pathConf *filer_pb.FilerConf_PathConf) {
	// Convert once to avoid allocation in multi-match case
	pathBytes := []byte(path)

	// Fast path: check if any rules match before allocating
	// This avoids allocation for paths with no configured rules (common case)
	var firstMatch *filer_pb.FilerConf_PathConf
	matchCount := 0

	fc.rules.MatchPrefix(pathBytes, func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		matchCount++
		if matchCount == 1 {
			firstMatch = value
			return true // continue to check for more matches
		}
		// Stop after 2 matches - we only need to know if there are multiple
		return false
	})

	// No rules match - return singleton (callers must NOT mutate)
	if matchCount == 0 {
		return emptyPathConf
	}

	// Single rule matches - return directly (callers must NOT mutate)
	if matchCount == 1 {
		return firstMatch
	}

	// Multiple rules match - need to merge (allocate new)
	pathConf = &filer_pb.FilerConf_PathConf{}
	fc.rules.MatchPrefix(pathBytes, func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		mergePathConf(pathConf, value)
		return true
	})
	return pathConf
}

// ClonePathConf creates a mutable copy of an existing PathConf.
// Use this when you need to modify a config (e.g., before calling SetLocationConf).
//
// IMPORTANT: Keep in sync with filer_pb.FilerConf_PathConf fields.
// When adding new fields to the protobuf, update this function accordingly.
func ClonePathConf(src *filer_pb.FilerConf_PathConf) *filer_pb.FilerConf_PathConf {
	if src == nil {
		return &filer_pb.FilerConf_PathConf{}
	}
	var worm *bool
	if src.Worm != nil {
		worm = proto.Bool(*src.Worm)
	}
	return &filer_pb.FilerConf_PathConf{
		LocationPrefix:           src.LocationPrefix,
		Collection:               src.Collection,
		Replication:              src.Replication,
		Ttl:                      src.Ttl,
		DiskType:                 src.DiskType,
		Fsync:                    src.Fsync,
		VolumeGrowthCount:        src.VolumeGrowthCount,
		ReadOnly:                 src.ReadOnly,
		MaxFileNameLength:        src.MaxFileNameLength,
		DataCenter:               src.DataCenter,
		Rack:                     src.Rack,
		DataNode:                 src.DataNode,
		DisableChunkDeletion:     src.DisableChunkDeletion,
		Worm:                     worm,
		WormGracePeriodSeconds:   src.WormGracePeriodSeconds,
		WormRetentionTimeSeconds: src.WormRetentionTimeSeconds,
	}
}

// ApplyBucketQuotaReadOnly sets read-only when usedSize exceeds quota and clears it
// once back under, reporting whether the flag changed. A non-positive quota is left
// untouched so a manually locked bucket is never reopened.
func (fc *FilerConf) ApplyBucketQuotaReadOnly(locationPrefix string, usedSize, quota float64) (readOnly, changed bool) {
	if quota <= 0 {
		return fc.MatchStorageRule(locationPrefix).ReadOnly, false
	}

	locConf := ClonePathConf(fc.MatchStorageRule(locationPrefix))
	locConf.LocationPrefix = locationPrefix
	wasReadOnly := locConf.ReadOnly

	if wasReadOnly {
		if usedSize < quota {
			locConf.ReadOnly = false
		}
	} else {
		if usedSize > quota {
			locConf.ReadOnly = true
		}
	}

	if locConf.ReadOnly == wasReadOnly {
		return wasReadOnly, false
	}
	fc.SetLocationConf(locConf)
	return locConf.ReadOnly, true
}

// ClearReadOnly clears the read-only flag on the rule at exactly locationPrefix,
// reporting whether the flag was set. This is the explicit unlock for a flag that
// ApplyBucketQuotaReadOnly can no longer clear once the quota is gone.
func (fc *FilerConf) ClearReadOnly(locationPrefix string) (changed bool) {
	locConf, found := fc.GetLocationConf(locationPrefix)
	if !found || !locConf.ReadOnly {
		return false
	}
	locConf.ReadOnly = false
	fc.SetLocationConf(locConf)
	return true
}

// ClearBucketReadOnly lifts the read-only flag that quota enforcement may have
// left on the bucket's path rule, saving the updated configuration back to the
// filer. It reports whether anything was cleared.
func ClearBucketReadOnly(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketsPath, bucketName string) (changed bool, err error) {
	data, err := ReadInsideFiler(ctx, client, DirectoryEtcSeaweedFS, FilerConfName)
	if err == filer_pb.ErrNotFound || (err == nil && len(data) == 0) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
	}
	fc := NewFilerConf()
	if err = fc.LoadFromBytes(data); err != nil {
		return false, fmt.Errorf("parse %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
	}
	// join the rule key exactly as s3.bucket.quota.enforce writes it, so the
	// exact-match lookup finds the rule even for a non-canonical bucketsPath
	if !fc.ClearReadOnly(bucketsPath + "/" + bucketName + "/") {
		return false, nil
	}
	var buf bytes.Buffer
	if err = fc.ToText(&buf); err != nil {
		return false, err
	}
	if err = SaveInsideFiler(ctx, client, DirectoryEtcSeaweedFS, FilerConfName, buf.Bytes()); err != nil {
		return false, err
	}
	return true, nil
}

// filerConfSnapshot is a read of filer.conf plus enough of the entry (or its
// absence) to make a follow-up write conditional via
// saveFilerConfConditionally, instead of blindly overwriting whatever is
// there by the time the write happens.
type filerConfSnapshot struct {
	fc    *FilerConf
	entry *filer_pb.Entry // nil if filer.conf did not exist at read time
}

func readFilerConfSnapshot(ctx context.Context, client filer_pb.SeaweedFilerClient) (*filerConfSnapshot, error) {
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: DirectoryEtcSeaweedFS,
		Name:      FilerConfName,
	})
	fc := NewFilerConf()
	if errors.Is(err, filer_pb.ErrNotFound) {
		return &filerConfSnapshot{fc: fc}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
	}
	if len(resp.Entry.Content) > 0 {
		if err := fc.LoadFromBytes(resp.Entry.Content); err != nil {
			return nil, fmt.Errorf("parse %s/%s: %v", DirectoryEtcSeaweedFS, FilerConfName, err)
		}
	}
	return &filerConfSnapshot{fc: fc, entry: resp.Entry}, nil
}

// saveFilerConfConditionally writes snap.fc back to filer.conf, conditioned
// on nothing having created or modified the file since snap was read. The
// filer evaluates the condition and applies the write atomically under its
// per-path lock (see UpdateEntry/CreateEntry in filer_grpc_server.go), so a
// concurrent writer that raced this one fails the precondition instead of
// silently having its change overwritten.
//
// The update path prefers an exact content check (IF_ETAG_MATCH keyed off an
// MD5 this function itself stamps into Attributes.Md5 on every write) over
// mtime, since mtime only has one-second resolution and says nothing about
// content. A filer.conf written before this code existed has no such hash
// yet, so that one write falls back to IF_UNMODIFIED_SINCE; every write from
// here on carries the hash, so subsequent calls use the exact check.
//
// That one bootstrap write is the sole remaining place a same-second race
// isn't caught: IF_ETAG_MATCH can't help there either, because an entry with
// no Md5 and no chunks hashes to the same fixed value regardless of its
// actual content (see filer.ETagChunks), so it can't distinguish two
// different same-second writes any better than mtime can. Closing that
// requires either an exact-content condition kind the filer protocol
// doesn't have, or unconditionally stamping Md5 on every plain
// SaveInsideFiler write everywhere in the codebase, not just here — out of
// scope for this fix. It self-heals after the first write either way.
func saveFilerConfConditionally(ctx context.Context, client filer_pb.SeaweedFilerClient, snap *filerConfSnapshot) error {
	var buf bytes.Buffer
	if err := snap.fc.ToText(&buf); err != nil {
		return err
	}
	content := buf.Bytes()
	contentMd5 := md5.Sum(content)

	if snap.entry == nil {
		err := filer_pb.CreateEntry(ctx, client, &filer_pb.CreateEntryRequest{
			Directory: DirectoryEtcSeaweedFS,
			Entry: &filer_pb.Entry{
				Name:        FilerConfName,
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0644),
					FileSize: uint64(len(content)),
					Md5:      contentMd5[:],
				},
				Content: content,
			},
			Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
				{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS},
			}},
		})
		if err != nil {
			return fmt.Errorf("filer.conf was created concurrently, retry: %w", err)
		}
		return nil
	}

	entry := snap.entry
	var condition *filer_pb.WriteCondition
	if entry.Attributes != nil && len(entry.Attributes.Md5) > 0 {
		condition = &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{fmt.Sprintf("%x", entry.Attributes.Md5)}},
		}}
	} else {
		var unmodifiedSince int64
		if entry.Attributes != nil {
			unmodifiedSince = entry.Attributes.Mtime
		} else {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		condition = &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: unmodifiedSince},
		}}
	}

	entry.Content = content
	entry.Attributes.Mtime = time.Now().Unix()
	entry.Attributes.FileSize = uint64(len(content))
	entry.Attributes.Md5 = contentMd5[:]
	err := filer_pb.UpdateEntry(ctx, client, &filer_pb.UpdateEntryRequest{
		Directory: DirectoryEtcSeaweedFS,
		Entry:     entry,
		Condition: condition,
	})
	if err != nil {
		return fmt.Errorf("filer.conf changed concurrently, retry: %w", err)
	}
	return nil
}

// ClearBucketLifecycleDayTTLs removes any day-TTL filer.conf rules a legacy
// PutBucketLifecycleConfiguration handler installed under the bucket's path.
// Per-write TTL is now driven by the LifecycleTTLResolver built off the
// stored lifecycle XML, so a lingering day-TTL rule would double-stamp
// expiration (volume server expires under the old rule) or contradict a
// newly saved XML. Returns the rules that were removed (nil if none), so a
// caller that needs to undo this can re-add exactly those rules with
// RestoreFilerConfLocationRules instead of overwriting the whole file with a
// stale snapshot that would clobber unrelated concurrent edits. The write
// itself is conditioned on filer.conf being unchanged since it was read here
// (see saveFilerConfConditionally), so a concurrent writer causes this call
// to fail rather than silently lose one side's change.
func ClearBucketLifecycleDayTTLs(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketsPath, bucketName, collection string) (removed []*filer_pb.FilerConf_PathConf, err error) {
	snap, err := readFilerConfSnapshot(ctx, client)
	if err != nil {
		return nil, err
	}
	if snap.entry == nil {
		return nil, nil
	}

	bucketPrefix := fmt.Sprintf("%s/%s/", bucketsPath, bucketName)
	for prefix, ttl := range snap.fc.GetCollectionTtls(collection) {
		if !strings.HasPrefix(prefix, bucketPrefix) || !strings.HasSuffix(ttl, "d") {
			continue
		}
		if locConf, found := snap.fc.GetLocationConf(prefix); found {
			removed = append(removed, ClonePathConf(locConf))
		}
		snap.fc.DeleteLocationConf(prefix)
	}
	if len(removed) == 0 {
		return nil, nil
	}

	if err := saveFilerConfConditionally(ctx, client, snap); err != nil {
		return nil, err
	}
	return removed, nil
}

// RestoreFilerConfLocationRules re-adds the given location rules into the
// current filer.conf, re-reading it fresh (and writing back conditionally,
// see saveFilerConfConditionally) so unrelated concurrent edits made since
// the rules were removed aren't clobbered by restoring a stale snapshot.
// Used to undo a ClearBucketLifecycleDayTTLs cleanup when the write it was
// guarding against double-stamped expiration for turns out not to have
// taken effect. No-op if rules is empty.
func RestoreFilerConfLocationRules(ctx context.Context, client filer_pb.SeaweedFilerClient, rules []*filer_pb.FilerConf_PathConf) error {
	if len(rules) == 0 {
		return nil
	}

	snap, err := readFilerConfSnapshot(ctx, client)
	if err != nil {
		return err
	}

	for _, rule := range rules {
		if err := snap.fc.SetLocationConf(rule); err != nil {
			return err
		}
	}

	return saveFilerConfConditionally(ctx, client, snap)
}

func (fc *FilerConf) GetCollectionTtls(collection string) (ttls map[string]string) {
	ttls = make(map[string]string)
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		if value.Collection == collection {
			ttls[value.LocationPrefix] = value.GetTtl()
		}
		return true
	})
	return ttls
}

// merge if values in b is not empty, merge them into a
func mergePathConf(a, b *filer_pb.FilerConf_PathConf) {
	a.Collection = util.Nvl(b.Collection, a.Collection)
	a.Replication = util.Nvl(b.Replication, a.Replication)
	a.Ttl = util.Nvl(b.Ttl, a.Ttl)
	a.DiskType = util.Nvl(b.DiskType, a.DiskType)
	a.Fsync = b.Fsync || a.Fsync
	if b.VolumeGrowthCount > 0 {
		a.VolumeGrowthCount = b.VolumeGrowthCount
	}
	a.ReadOnly = b.ReadOnly || a.ReadOnly
	if b.MaxFileNameLength > 0 {
		a.MaxFileNameLength = b.MaxFileNameLength
	}
	a.DataCenter = util.Nvl(b.DataCenter, a.DataCenter)
	a.Rack = util.Nvl(b.Rack, a.Rack)
	a.DataNode = util.Nvl(b.DataNode, a.DataNode)
	a.DisableChunkDeletion = b.DisableChunkDeletion || a.DisableChunkDeletion
	// worm merges on presence, so a nested rule can turn it off. readOnly, fsync and
	// disableChunkDeletion stay OR'ed on purpose: a nested rule must not be able to
	// lift a lock the bucket set.
	if b.Worm != nil {
		// copy the value: a is often a scratch conf while b is a live trie entry
		a.Worm = proto.Bool(*b.Worm)
	}
	if b.WormRetentionTimeSeconds > 0 {
		a.WormRetentionTimeSeconds = b.WormRetentionTimeSeconds
	}
	if b.WormGracePeriodSeconds > 0 {
		a.WormGracePeriodSeconds = b.WormGracePeriodSeconds
	}
}

func (fc *FilerConf) ToProto() *filer_pb.FilerConf {
	m := &filer_pb.FilerConf{Version: FilerConfVersion}
	fc.rules.Walk(func(key []byte, value *filer_pb.FilerConf_PathConf) bool {
		m.Locations = append(m.Locations, value)
		return true
	})
	return m
}

func (fc *FilerConf) ToText(writer io.Writer) error {
	return ProtoToText(writer, fc.ToProto())
}
