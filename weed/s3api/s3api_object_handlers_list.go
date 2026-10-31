package s3api

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type OptionalString struct {
	string
	set bool
}

func (o OptionalString) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if !o.set {
		return nil
	}
	return e.EncodeElement(o.string, startElement)
}

type ListBucketResultV2 struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	MaxKeys               uint16         `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []ListEntry    `xml:"Contents,omitempty"`
	CommonPrefixes        []PrefixEntry  `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     OptionalString `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
}

type listBucketResultV1 struct {
	XMLName        xml.Name        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Metadata       []MetadataEntry `xml:"Metadata,omitempty"`
	Name           string          `xml:"Name"`
	Prefix         string          `xml:"Prefix"`
	Marker         string          `xml:"Marker"`
	NextMarker     string          `xml:"NextMarker,omitempty"`
	MaxKeys        int             `xml:"MaxKeys"`
	Delimiter      string          `xml:"Delimiter,omitempty"`
	IsTruncated    bool            `xml:"IsTruncated"`
	Contents       []ListEntry     `xml:"Contents,omitempty"`
	CommonPrefixes []PrefixEntry   `xml:"CommonPrefixes,omitempty"`
	EncodingType   string          `xml:"EncodingType,omitempty"`
}

func toListBucketResultV1(in ListBucketResult) listBucketResultV1 {
	return listBucketResultV1{
		Metadata:       in.Metadata,
		Name:           in.Name,
		Prefix:         in.Prefix,
		Marker:         encodeMarkerEcho(in.Marker, in.EncodingType == s3.EncodingTypeUrl),
		NextMarker:     encodeMarkerEcho(in.NextMarker, in.EncodingType == s3.EncodingTypeUrl),
		MaxKeys:        in.MaxKeys,
		Delimiter:      in.Delimiter,
		IsTruncated:    in.IsTruncated,
		Contents:       in.Contents,
		CommonPrefixes: in.CommonPrefixes,
		EncodingType:   in.EncodingType,
	}
}

func (s3a *S3ApiServer) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	originalPrefix, startAfter, delimiter, continuationToken, encodingTypeUrl, fetchOwner, maxKeys, allowUnordered, errCode := getListObjectsV2Args(r.URL.Query())

	glog.V(2).Infof("ListObjectsV2Handler bucket=%s prefix=%s marker=%s", bucket, originalPrefix, continuationToken.string)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// maxKeys is uint16 here; negative values are rejected during parsing.

	// AWS S3 compatibility: allow-unordered cannot be used with delimiter
	if allowUnordered && delimiter != "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidUnorderedWithDelimiter)
		return
	}

	marker := decodeListContinuationToken(continuationToken.string)
	if !continuationToken.set {
		marker = startAfter
	}

	// Adjust marker if it ends with delimiter to skip all entries with that prefix
	requestMarker := marker
	marker = adjustMarkerForDelimiter(marker, originalPrefix, delimiter)

	response, err := s3a.listFilerEntries(r.Context(), listObjectsRequest{
		bucket:          bucket,
		prefix:          originalPrefix,
		marker:          marker,
		requestMarker:   requestMarker,
		delimiter:       delimiter,
		maxKeys:         maxKeys,
		encodingTypeUrl: encodingTypeUrl,
		fetchOwner:      fetchOwner,
	})

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if len(response.Contents) == 0 {
		if exists, existErr := s3a.bucketExists(bucket); existErr == nil && !exists {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
	}

	responseV2 := &ListBucketResultV2{
		Name:                  response.Name,
		CommonPrefixes:        response.CommonPrefixes,
		Contents:              response.Contents,
		ContinuationToken:     continuationToken,
		Delimiter:             response.Delimiter,
		IsTruncated:           response.IsTruncated,
		KeyCount:              len(response.Contents) + len(response.CommonPrefixes),
		MaxKeys:               uint16(response.MaxKeys),
		NextContinuationToken: encodeListContinuationToken(response.NextMarker),
		Prefix:                response.Prefix,
		StartAfter:            startAfter,
	}
	if encodingTypeUrl {
		responseV2.EncodingType = s3.EncodingTypeUrl
		responseV2.StartAfter = urlPathEscape(startAfter)
	}

	glog.V(3).Infof("ListObjectsV2Handler response: %+v", responseV2)
	writeSuccessResponseXML(w, r, responseV2)
}

// continuationTokenEncodingPrefix tags a NextContinuationToken whose payload is the
// base64url encoding of the raw key. XML 1.0 cannot carry control characters, and the
// XML encoder replaces them with U+FFFD, so a token holding such a key would resume
// past a mangled name and skip every key sorting between the two. Keys that are valid
// XML text still travel as the bare key, so the token is unchanged for them.
const continuationTokenEncodingPrefix = "base64:"

func encodeListContinuationToken(key string) string {
	if key == "" {
		return ""
	}
	if isXMLText(key) && !strings.HasPrefix(key, continuationTokenEncodingPrefix) {
		return key
	}
	return continuationTokenEncodingPrefix + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func encodeListContinuationTokenField(value *string) *string {
	if value == nil {
		return nil
	}
	return aws.String(encodeListContinuationToken(*value))
}

// encodeMarkerEcho renders a decoded request marker for the response echo:
// the token encoding keeps an XML-unsafe key legible, and urlPathEscape applies
// the encoding-type=url form the keys in the same response already carry.
func encodeMarkerEcho(marker string, encodingTypeUrl bool) string {
	marker = encodeListContinuationToken(marker)
	if encodingTypeUrl {
		marker = urlPathEscape(marker)
	}
	return marker
}

func decodeEscapedEcho(marker string, encodingTypeUrl bool, escape func(string) string) string {
	if !encodingTypeUrl {
		return marker
	}
	if unescaped, err := url.QueryUnescape(marker); err == nil && escape(unescaped) == marker {
		return unescaped
	}
	return marker
}

func decodeListContinuationToken(token string) string {
	key, ok := strings.CutPrefix(token, continuationTokenEncodingPrefix)
	if !ok {
		return token
	}
	decoded, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		// Not a token this server wrote; resume at the token as the raw key.
		return token
	}
	decodedKey := string(decoded)
	// encodeListContinuationToken only writes a token for a key that cannot
	// travel bare, so a decoded key that could have stayed bare — valid XML
	// text without the prefix — means the input was a legacy raw-key token,
	// not something to decode.
	if isXMLText(decodedKey) && !strings.HasPrefix(decodedKey, continuationTokenEncodingPrefix) {
		return token
	}
	// A decoded key that could not travel bare makes the token ambiguous
	// ("base64:AQ" reads as both the encoded form of "\x01" and a literal
	// legacy key). The encoded reading wins: preferring the raw one would
	// resume past real entries for every token this server writes, while a
	// legacy token aliasing the encoded form only re-lists keys.
	return decodedKey
}

// isXMLText reports whether s survives an XML 1.0 round trip unchanged, i.e. the
// encoder does not replace any of its characters with U+FFFD.
func isXMLText(s string) bool {
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			return false
		}
		if !isXMLChar(r) {
			return false
		}
		i += size
	}
	return true
}

// isXMLChar follows the XML 1.0 Char production the encoding/xml encoder enforces.
func isXMLChar(r rune) bool {
	switch {
	case r == 0x9 || r == 0xA || r == 0xD:
		return true
	case r >= 0x20 && r <= 0xD7FF:
		return true
	case r >= 0xE000 && r <= 0xFFFD:
		return true
	case r >= 0x10000 && r <= 0x10FFFF:
		return true
	}
	return false
}

func (s3a *S3ApiServer) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	originalPrefix, listMarker, delimiter, encodingTypeUrl, maxKeys, allowUnordered, errCode := getListObjectsV1Args(r.URL.Query())

	glog.V(2).Infof("ListObjectsV1Handler bucket=%s prefix=%s marker=%s delimiter=%s maxKeys=%d", bucket, originalPrefix, listMarker, delimiter, maxKeys)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// maxKeys is uint16 here; negative values are rejected during parsing.

	// AWS S3 compatibility: allow-unordered cannot be used with delimiter
	if allowUnordered && delimiter != "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidUnorderedWithDelimiter)
		return
	}

	// Adjust marker if it ends with delimiter to skip all entries with that prefix
	requestMarker := decodeListContinuationToken(listMarker)
	marker := adjustMarkerForDelimiter(requestMarker, originalPrefix, delimiter)

	response, err := s3a.listFilerEntries(r.Context(), listObjectsRequest{
		bucket:          bucket,
		prefix:          originalPrefix,
		marker:          marker,
		requestMarker:   requestMarker,
		delimiter:       delimiter,
		maxKeys:         maxKeys,
		encodingTypeUrl: encodingTypeUrl,
		fetchOwner:      true,
	})

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	sanitizeV1MarkerEcho(&response, marker, encodingTypeUrl)

	if len(response.Contents) == 0 {
		if exists, existErr := s3a.bucketExists(bucket); existErr == nil && !exists {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
	}

	glog.V(3).Infof("ListObjectsV1Handler response: %+v", response)
	writeSuccessResponseXML(w, r, toListBucketResultV1(response))
}

func sanitizeV1MarkerEcho(response *ListBucketResult, marker string, encodingTypeUrl bool) {
	if marker == "" {
		return
	}

	// Content keys travel in the response wire encoding while NextMarker stays
	// the raw key, so each compares against the marker in its own form.
	rawCandidates := map[string]struct{}{
		marker:                          {},
		strings.TrimPrefix(marker, "/"): {},
	}
	keyCandidates := rawCandidates
	if encodingTypeUrl {
		keyCandidates = make(map[string]struct{}, len(rawCandidates))
		for m := range rawCandidates {
			keyCandidates[urlPathEscape(m)] = struct{}{}
		}
	}
	matchesMarker := func(candidates map[string]struct{}, v string) bool {
		if _, ok := candidates[v]; ok {
			return true
		}
		_, ok := candidates[strings.TrimPrefix(v, "/")]
		return ok
	}

	if len(response.Contents) > 0 {
		filtered := response.Contents[:0]
		for _, content := range response.Contents {
			if matchesMarker(keyCandidates, content.Key) {
				continue
			}
			filtered = append(filtered, content)
		}
		response.Contents = filtered
	}

	// doListFilerEntries advances nextMarker to the last emitted entry and skips
	// the marker in exclusive mode. So NextMarker==marker indicates no progress.
	if matchesMarker(rawCandidates, response.NextMarker) && len(response.Contents) == 0 && len(response.CommonPrefixes) == 0 {
		response.NextMarker = ""
		response.IsTruncated = false
	}
}

type listObjectsRequest struct {
	bucket string
	prefix string
	marker string
	// requestMarker is the marker as the client sent it, before a marker ending on the
	// delimiter was trimmed to the walk's cutoff. The response echoes it, and no key it
	// names is listed.
	requestMarker   string
	delimiter       string
	maxKeys         uint16
	encodingTypeUrl bool
	fetchOwner      bool
}

func (s3a *S3ApiServer) listFilerEntries(ctx context.Context, req listObjectsRequest) (response ListBucketResult, err error) {
	bucket, originalPrefix, originalMarker := req.bucket, req.prefix, req.marker
	maxKeys, delimiter := req.maxKeys, req.delimiter
	encodingTypeUrl, fetchOwner := req.encodingTypeUrl, req.fetchOwner
	requestMarker := req.requestMarker
	if requestMarker == "" {
		requestMarker = originalMarker
	}
	excludedKey := excludedMarkerKey(requestMarker, originalMarker)
	// convert full path prefix into directory name and prefix for entry name
	requestDir, prefix, marker, prefixEndsOnDelimiter := normalizePrefixMarker(originalPrefix, originalMarker)
	bucketPrefix := s3a.bucketPrefix(bucket)
	reqDir := bucketPrefix[:len(bucketPrefix)-1]
	if requestDir != "" {
		reqDir = fmt.Sprintf("%s%s", bucketPrefix, requestDir)
	}

	var contents []ListEntry
	var commonPrefixes []PrefixEntry
	var doErr error
	var nextMarker string
	cursor := &ListingCursor{
		maxKeys:               maxKeys,
		prefixEndsOnDelimiter: prefixEndsOnDelimiter,
	}

	// Special case: when maxKeys = 0, return empty results immediately with IsTruncated=false
	if maxKeys == 0 {
		response = ListBucketResult{
			Name:           bucket,
			Prefix:         originalPrefix,
			Marker:         requestMarker,
			NextMarker:     "",
			MaxKeys:        int(maxKeys),
			Delimiter:      delimiter,
			IsTruncated:    false,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}
		if encodingTypeUrl {
			response.EncodingType = s3.EncodingTypeUrl
		}
		return
	}

	alignedMarker := marker

	// check filer
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// The failover wrapper retries this callback on another filer after a
		// transport error, so every attempt rebuilds the page from scratch: a
		// partially built page must not leak into the retry.
		contents = nil
		commonPrefixes = nil
		doErr = nil
		nextMarker = ""
		marker = alignedMarker
		*cursor = ListingCursor{
			maxKeys:               maxKeys,
			prefixEndsOnDelimiter: prefixEndsOnDelimiter,
		}

		var lastEntryWasCommonPrefix bool
		var lastCommonPrefix string
		// Backing for the newest CommonPrefix: which unsettled null objects stand
		// behind it, and whether anything definitely listable does. A prefix whose
		// null backers all settle as delete-marked names nothing and is retracted;
		// tracking backers by key keeps a marker that never joined the prefix (a
		// version-only key with no base object) from debiting it. Contributors to
		// a prefix stream contiguously, so only the newest prefix needs this.
		var lastPrefixNullBackers map[string]bool
		var lastPrefixConfirmed bool

		// Hoist versioning check out of per-entry callback
		versioningState, _ := s3a.getVersioningState(bucket)
		// Suspending versioning keeps the .versions directories already written, so both
		// states can emit a key twice (base-path null object plus its .versions sibling)
		// and can hold a directory whose objects are all gone from the current-version view.
		versioningConfigured := versioningState != ""
		cursor.hideDeletedPrefixes = versioningConfigured

		// Helper function to handle dedup/append logic
		appendOrDedup := func(newEntry ListEntry) {
			if versioningConfigured {
				// A key's .versions sibling resolves after every key that sorts between
				// them ("k.bak" lists between "k" and "k.versions"), so the base entry is
				// found by scanning back through the page and a late resolution is
				// inserted where it keeps the page sorted, not at the end.
				insertAt := len(contents)
				for insertAt > 0 && contents[insertAt-1].Key > newEntry.Key {
					insertAt--
				}
				if insertAt > 0 && contents[insertAt-1].Key == newEntry.Key {
					glog.V(3).Infof("listFilerEntries deduplicating versioned entry: %s", newEntry.Key)
					contents[insertAt-1] = newEntry
					return
				}
				contents = append(contents, ListEntry{})
				copy(contents[insertAt+1:], contents[insertAt:])
				contents[insertAt] = newEntry
				cursor.maxKeys--
			} else {
				contents = append(contents, newEntry)
				cursor.maxKeys--
			}
		}

		// Null objects whose .versions sibling has not streamed yet, and so may still
		// turn out to be shadowed by a delete marker. Entries stream in order, so a
		// pending null is dropped once the stream passes its sibling's name; the cap
		// only binds on pathological prefix nests and falls back to sibling-adjacent
		// behavior for the evicted oldest.
		type pendingNull struct{ dir, name string }
		var pendingNulls []pendingNull
		prunePendingNulls := func(dir, passedName string) {
			kept := pendingNulls[:0]
			for _, p := range pendingNulls {
				if p.dir != dir || p.name+s3_constants.VersionsFolder >= passedName {
					kept = append(kept, p)
				}
			}
			pendingNulls = kept
		}
		dropPendingNull := func(dir, name string) {
			kept := pendingNulls[:0]
			for _, p := range pendingNulls {
				if p.dir != dir || p.name != name {
					kept = append(kept, p)
				}
			}
			pendingNulls = kept
		}
		// prefixForKey returns the CommonPrefix a key folds into under the request's
		// delimiter, derived exactly as the emission sites derive it, or "".
		prefixForKey := func(dir, name string) string {
			if delimiter == "" {
				return ""
			}
			undelimited := strings.TrimPrefix((dir + "/" + name)[len(bucketPrefix):], originalPrefix)
			if parts := strings.SplitN(undelimited, delimiter, 2); len(parts) == 2 {
				return originalPrefix + parts[0] + delimiter
			}
			return ""
		}

		// addCommonPrefix records a CommonPrefix the page has not emitted yet. maxKeys is
		// unsigned, so the decrement is clamped rather than allowed to wrap: a prefix
		// object spends two slots on one entry, and the budget is only checked between
		// entries.
		addCommonPrefix := func(prefix string) {
			for i := range commonPrefixes {
				if commonPrefixes[i].Prefix == prefix {
					if prefix == lastCommonPrefix {
						lastPrefixConfirmed = true
					}
					return
				}
			}
			commonPrefixes = append(commonPrefixes, PrefixEntry{Prefix: prefix})
			if cursor.maxKeys > 0 {
				cursor.maxKeys--
			}
			lastEntryWasCommonPrefix = true
			lastCommonPrefix = prefix
			lastPrefixNullBackers, lastPrefixConfirmed = nil, true
		}

		retractPrefixBacking := func(prefix, dir, name string) {
			if prefix != lastCommonPrefix || !lastPrefixNullBackers[dir+"/"+name] {
				return
			}
			delete(lastPrefixNullBackers, dir+"/"+name)
			if len(lastPrefixNullBackers) == 0 && !lastPrefixConfirmed &&
				len(commonPrefixes) > 0 && commonPrefixes[len(commonPrefixes)-1].Prefix == prefix {
				commonPrefixes = commonPrefixes[:len(commonPrefixes)-1]
				cursor.maxKeys++
				lastEntryWasCommonPrefix = false
				lastCommonPrefix = ""
			}
		}

		// The null object for a key lists before its .versions sibling can reveal
		// that the current version is a delete marker, so the reveal retracts it -
		// from the page's keys, or from the CommonPrefix it was folded into.
		cursor.retractEntry = func(dir, name string) {
			dropPendingNull(dir, name)
			dirName, entryName, _ := entryUrlEncode(dir, name, encodingTypeUrl)
			key := fmt.Sprintf("%s/%s", dirName, entryName)[len(bucketPrefix):]
			for i := len(contents) - 1; i >= 0 && contents[i].Key >= key; i-- {
				if contents[i].Key == key {
					contents = append(contents[:i], contents[i+1:]...)
					cursor.maxKeys++
					return
				}
			}
			if prefix := prefixForKey(dir, name); prefix != "" {
				retractPrefixBacking(prefix, dir, name)
			}
		}

		// Only a definitive not-found means the null object is live; a transient
		// failure leaves the entry unsettled and must not commit it to the page,
		// since the next page would then skip the sibling for good.
		settlePendingNull := func(p pendingNull) error {
			versionsEntry, err := s3a.getEntry(p.dir, p.name+s3_constants.VersionsFolder)
			if err != nil {
				if errors.Is(err, filer_pb.ErrNotFound) || status.Code(err) == codes.NotFound {
					return nil
				}
				return fmt.Errorf("settle null object %s/%s: %w", p.dir, p.name, err)
			}
			fullObjectPath := strings.TrimPrefix(p.dir+"/"+p.name, bucketPrefix)
			latest, lerr := s3a.getLatestVersionEntryFromDirectoryEntry(bucket, fullObjectPath, versionsEntry)
			switch {
			case lerr == nil:
				if prefix := prefixForKey(p.dir, p.name); prefix != "" {
					// The key folds into a CommonPrefix; a live current version
					// confirms the prefix rather than surfacing the key.
					if prefix == lastCommonPrefix {
						lastPrefixConfirmed = true
					}
				} else {
					dirName, entryName, _ := entryUrlEncode(p.dir, latest.Name, encodingTypeUrl)
					appendOrDedup(newListEntry(s3a, latest, "", dirName, entryName, bucketPrefix, fetchOwner, false, false))
				}
			case errors.Is(lerr, ErrDeleteMarker), errors.Is(lerr, filer_pb.ErrNotFound):
				cursor.retractEntry(p.dir, p.name)
			default:
				return fmt.Errorf("settle null object %s/%s: %w", p.dir, p.name, lerr)
			}
			return nil
		}

		addPendingNull := func(dir, name string) {
			if len(pendingNulls) >= 8 {
				// Settle rather than silently evict: an unsettled null would leak past
				// the page, and the resume skip would then keep it stale for good. A
				// failed settlement is retained for page-close resolution to retry.
				settled := pendingNulls[0]
				pendingNulls = pendingNulls[1:]
				if settleErr := settlePendingNull(settled); settleErr != nil {
					pendingNulls = append([]pendingNull{settled}, pendingNulls...)
				}
			}
			pendingNulls = append(pendingNulls, pendingNull{dir, name})
		}

		// A page may fill while a trailing null object's .versions sibling is still
		// unstreamed; settle each one by direct lookup before the page is declared
		// final. A retraction here reopens the page's quota, and a failure fails the
		// listing rather than committing an unsettled entry.
		cursor.resolvePendingNulls = func() error {
			for len(pendingNulls) > 0 {
				p := pendingNulls[0]
				pendingNulls = pendingNulls[1:]
				if settleErr := settlePendingNull(p); settleErr != nil {
					pendingNulls = append([]pendingNull{p}, pendingNulls...)
					return settleErr
				}
			}
			return nil
		}

		for {
			empty := true

			nextMarker, doErr = s3a.doListFilerEntries(ctx, client, listDirectoryRequest{dir: reqDir, prefix: prefix, marker: marker, delimiter: delimiter, bucket: bucket}, cursor, func(dir string, entry *filer_pb.Entry) {
				empty = false
				prunePendingNulls(dir, entry.Name)
				if excludedKey != "" && !entry.IsDirectory && fmt.Sprintf("%s/%s", dir, entry.Name)[len(bucketPrefix):] == excludedKey {
					return
				}
				// A delimiter marker excludes the entire folded prefix, not just
				// the key bearing its name. This also applies to delimiters such as
				// "_", whose groups are not directory blocks in the filer walk.
				if delimiter != "" {
					name := entry.Name
					if entry.IsDirectory && !entry.IsPrefixObject() {
						name += "/"
					}
					if folded := prefixForKey(dir, name); folded != "" && folded <= requestMarker {
						return
					}
				}
				dirName, entryName, _ := entryUrlEncode(dir, entry.Name, encodingTypeUrl)
				if entry.IsDirectory {
					if originalPrefix != "" {
						normalizedPrefix := strings.TrimPrefix(strings.TrimSuffix(originalPrefix, "/"), "/")
						if normalizedPrefix != "" {
							relativePath := strings.TrimPrefix(fmt.Sprintf("%s/%s", dir, entry.Name), bucketPrefix)
							relativePath = strings.TrimPrefix(relativePath, "/")
							if normalizedPrefix == relativePath && !s3a.hasChildren(ctx, bucket, relativePath) && !entry.IsDirectoryKeyObject() {
								return
							}
						}
					}
					// A prefix object's key carries no trailing slash, so it lists as a
					// plain key. The traversal never descends into it under a delimiter,
					// so the CommonPrefix its nested keys fold into is added here too.
					if entry.IsPrefixObject() {
						keyOnly := cursor.prefixObjectKeyOnly
						cursor.prefixObjectKeyOnly = false
						key := fmt.Sprintf("%s/%s", dir, entry.Name)[len(bucketPrefix):]
						if strings.HasPrefix(key, originalPrefix) {
							if folded := prefixForKey(dir, entry.Name); folded != "" {
								// The key and everything under it fold into the same prefix.
								addCommonPrefix(folded)
								return
							}
							appendOrDedup(newListEntry(s3a, entry, "", dirName, entryName, bucketPrefix, fetchOwner, false, false))
							lastEntryWasCommonPrefix = false
						}
						// The key and the prefix come off one entry, which a marker names as a
						// whole, so a page ending between them cannot resume at the prefix:
						// a marker of "<key>/" means the subtree is done, not that it is
						// next. The page runs one item over maxKeys instead of dropping it.
						if !keyOnly {
							if childPrefix := prefixForKey(dir, entry.Name+"/"); childPrefix != "" && s3a.hasChildren(ctx, bucket, key) {
								addCommonPrefix(childPrefix)
							}
						}
						return
					}
					// When delimiter is specified, apply delimiter logic to directory key objects too
					if delimiter != "" && entry.IsDirectoryKeyObject() {
						// Apply the same delimiter logic as for regular files
						var delimiterFound bool
						// Use raw dir and entry.Name (not encoded) to ensure consistent handling
						// Encoding will be applied after sorting if encodingTypeUrl is set
						undelimitedPath := fmt.Sprintf("%s/%s/", dir, entry.Name)[len(bucketPrefix):]

						// take into account a prefix if supplied while delimiting.
						undelimitedPath = strings.TrimPrefix(undelimitedPath, originalPrefix)

						delimitedPath := strings.SplitN(undelimitedPath, delimiter, 2)
						if len(delimitedPath) == 2 {
							// S3 clients expect the delimited prefix to contain the delimiter and prefix.
							delimitedPrefix := originalPrefix + delimitedPath[0] + delimiter

							// Check if this CommonPrefix already exists
							if !lastEntryWasCommonPrefix || lastCommonPrefix != delimitedPrefix {
								// New CommonPrefix found
								commonPrefixes = append(commonPrefixes, PrefixEntry{
									Prefix: delimitedPrefix,
								})
								cursor.maxKeys--
								delimiterFound = true
								lastEntryWasCommonPrefix = true
								lastCommonPrefix = delimitedPrefix
								lastPrefixNullBackers, lastPrefixConfirmed = nil, true
							} else {
								// This directory object belongs to an existing CommonPrefix, skip it
								delimiterFound = true
								lastPrefixConfirmed = true
							}
						}

						// If no delimiter found in the directory object name, treat it as a regular key
						if !delimiterFound {
							newEntry := newListEntry(s3a, entry, "", dirName, entryName, bucketPrefix, fetchOwner, true, false)
							appendOrDedup(newEntry)
							lastEntryWasCommonPrefix = false
						}
					} else if entry.IsDirectoryKeyObject() {
						// No delimiter specified, or delimiter doesn't apply - treat as regular key
						newEntry := newListEntry(s3a, entry, "", dirName, entryName, bucketPrefix, fetchOwner, true, false)
						appendOrDedup(newEntry)
						lastEntryWasCommonPrefix = false
						// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
					} else if delimiter != "" { // A response can contain CommonPrefixes only if you specify a delimiter.
						// Use raw dir and entry.Name (not encoded) to ensure consistent handling
						// Encoding will be applied after sorting if encodingTypeUrl is set
						dirPrefix := fmt.Sprintf("%s/%s/", dir, entry.Name)[len(bucketPrefix):]
						commonPrefixes = append(commonPrefixes, PrefixEntry{
							Prefix: dirPrefix,
						})
						//All of the keys (up to 1,000) rolled up into a common prefix count as a single return when calculating the number of returns.
						cursor.maxKeys--
						lastEntryWasCommonPrefix = true
						lastCommonPrefix = dirPrefix
						lastPrefixNullBackers, lastPrefixConfirmed = nil, true
					}
				} else {
					var delimiterFound bool
					if delimiter != "" {
						// keys that contain the same string between the prefix and the first occurrence of the delimiter are grouped together as a commonPrefix.
						// extract the string between the prefix and the delimiter and add it to the commonPrefixes if it's unique.
						undelimitedPath := fmt.Sprintf("%s/%s", dir, entry.Name)[len(bucketPrefix):]

						// take into account a prefix if supplied while delimiting.
						undelimitedPath = strings.TrimPrefix(undelimitedPath, originalPrefix)

						delimitedPath := strings.SplitN(undelimitedPath, delimiter, 2)

						if len(delimitedPath) == 2 {
							// S3 clients expect the delimited prefix to contain the delimiter and prefix.
							delimitedPrefix := originalPrefix + delimitedPath[0] + delimiter

							// A null object rolled into a prefix still awaits its .versions
							// sibling, and the prefix must not outlive its only backers.
							isNullBacker := false
							if versioningConfigured {
								if vid := string(entry.Extended[s3_constants.ExtVersionIdKey]); vid == "" || vid == "null" {
									isNullBacker = true
								}
							}

							for i := range commonPrefixes {
								if commonPrefixes[i].Prefix == delimitedPrefix {
									delimiterFound = true
									break
								}
							}

							if !delimiterFound {
								commonPrefixes = append(commonPrefixes, PrefixEntry{
									Prefix: delimitedPrefix,
								})
								cursor.maxKeys--
								delimiterFound = true
								lastEntryWasCommonPrefix = true
								lastCommonPrefix = delimitedPrefix
								lastPrefixNullBackers, lastPrefixConfirmed = nil, !isNullBacker
								if isNullBacker {
									lastPrefixNullBackers = map[string]bool{dir + "/" + entry.Name: true}
									addPendingNull(dir, entry.Name)
								}
							} else {
								// This object belongs to an existing CommonPrefix, skip it
								// but continue processing to maintain correct flow
								delimiterFound = true
								if delimitedPrefix == lastCommonPrefix {
									if isNullBacker {
										if lastPrefixNullBackers == nil {
											lastPrefixNullBackers = map[string]bool{}
										}
										lastPrefixNullBackers[dir+"/"+entry.Name] = true
										addPendingNull(dir, entry.Name)
									} else {
										lastPrefixConfirmed = true
										dropPendingNull(dir, entry.Name)
									}
								}
							}
						}
					}
					if !delimiterFound {
						glog.V(4).Infof("Adding file to contents: %s", entryName)
						newEntry := newListEntry(s3a, entry, "", dirName, entryName, bucketPrefix, fetchOwner, false, false)
						appendOrDedup(newEntry)
						lastEntryWasCommonPrefix = false
						if versioningConfigured {
							// A real version id means the .versions sibling resolved this
							// key; anything else is a null object the sibling may shadow.
							if vid := string(entry.Extended[s3_constants.ExtVersionIdKey]); vid == "" || vid == "null" {
								addPendingNull(dir, entry.Name)
							} else {
								dropPendingNull(dir, entry.Name)
							}
						}
					}
				}
			})
			if doErr != nil {
				if errors.Is(doErr, filer_pb.ErrNotFound) {
					empty = true
					nextMarker = ""
					break
				}
				return doErr
			}

			if cursor.isTruncated {
				nextMarker = buildTruncatedNextMarker(requestDir, nextMarker, lastEntryWasCommonPrefix, lastCommonPrefix)
			}

			if cursor.isTruncated {
				break
			} else if empty || strings.HasSuffix(originalPrefix, "/") {
				nextMarker = ""
				break
			} else {
				// start next loop
				marker = nextMarker
			}
		}

		response = ListBucketResult{
			Name:           bucket,
			Prefix:         originalPrefix,
			Marker:         requestMarker,
			NextMarker:     nextMarker,
			MaxKeys:        int(maxKeys),
			Delimiter:      delimiter,
			IsTruncated:    cursor.isTruncated,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}
		// Sort CommonPrefixes to match AWS S3 behavior
		// AWS S3 treats the delimiter character specially for sorting common prefixes.
		// For example, with delimiter '/', 'foo/' should come before 'foo+1/' even though '+' (ASCII 43) < '/' (ASCII 47).
		// This custom comparison ensures correct S3-compatible lexicographical ordering.
		sort.Slice(response.CommonPrefixes, func(i, j int) bool {
			return compareWithDelimiter(response.CommonPrefixes[i].Prefix, response.CommonPrefixes[j].Prefix, delimiter)
		})

		// URL-encode CommonPrefixes AFTER sorting (if EncodingType=url)
		// This ensures proper sort order (on decoded values) and correct encoding in response
		if encodingTypeUrl {
			response.EncodingType = s3.EncodingTypeUrl
			for i := range response.CommonPrefixes {
				response.CommonPrefixes[i].Prefix = urlPathEscape(response.CommonPrefixes[i].Prefix)
			}
		}
		return nil
	})

	return
}

type ListingCursor struct {
	maxKeys               uint16
	isTruncated           bool
	prefixEndsOnDelimiter bool
	// hideDeletedPrefixes turns on the dirHoldsOnlyHiddenEntries probe, which only has
	// something to find once a bucket has version history to leave behind.
	hideDeletedPrefixes bool
	probedEntries       int
	// probeBudget overrides hiddenProbeBudget for the per-prefix probe; 0 means use
	// the package default. Tests set this to a small value to stay fast.
	probeBudget int
	// probeTotalBudget overrides hiddenProbeTotalBudget for the request-wide probe
	// ceiling; 0 means use the package default.
	probeTotalBudget int
	// retractEntry undoes the listing of a base-path null object once its .versions
	// sibling reveals that the current version is a delete marker.
	retractEntry func(dir, name string)
	// resolvePendingNulls settles trailing null objects whose .versions sibling
	// has not streamed yet before a page is declared full.
	resolvePendingNulls func() error
	// prefixObjectKeyOnly limits the next prefix-object entry passed to the entry
	// callback to its plain key; its "<name>/" CommonPrefix is deferred to the
	// directory's own block position.
	prefixObjectKeyOnly bool
}

// excludedMarkerKey returns the key an exclusive marker names that the walk's cutoff no
// longer excludes, because a marker ending on the delimiter is trimmed to that cutoff.
// The key is skipped as it streams, rather than spending a slot of the page and being
// dropped from the answer afterwards, which would turn a truncated page into a final one.
func excludedMarkerKey(requestMarker, marker string) string {
	if requestMarker == marker {
		return ""
	}
	return strings.TrimLeft(requestMarker, "/")
}

// markerSortsBeforePrefix reports whether marker is a cutoff that excludes no key
// under prefix: the marker sorts before the prefix and is not under it, so every key
// carrying the prefix already sorts after it. A marker that sorts after the prefix is
// left alone: it may sit inside a partial name prefix's match set ("parent" also
// matches "parentDir/…"), which normalizePrefixMarker handles.
func markerSortsBeforePrefix(prefix, marker string) bool {
	prefix = strings.TrimLeft(prefix, "/")
	marker = strings.TrimLeft(marker, "/")
	if marker == "" || prefix == "" {
		return false
	}
	return !strings.HasPrefix(marker, prefix) && marker < prefix
}

// markerSortsPastPrefix reports whether marker lies beyond the last key the
// prefix can match. A marker that diverges from the prefix at a larger byte
// is after every key under it, so the listing is empty no matter where the
// walk would resume.
func markerSortsPastPrefix(prefix, marker string) bool {
	prefix = strings.TrimLeft(prefix, "/")
	marker = strings.TrimLeft(marker, "/")
	if marker == "" || prefix == "" {
		return false
	}
	return !strings.HasPrefix(marker, prefix) && marker > prefix
}

// the prefix and marker may be in different directories
// normalizePrefixMarker ensures the prefix and marker both starts from the same directory.
// prefixEndsOnDelimiter tells the walk that the prefix names one directory, whose own key
// is in scope.
func normalizePrefixMarker(prefix, marker string) (alignedDir, alignedPrefix, alignedMarker string, prefixEndsOnDelimiter bool) {
	// A marker that excludes no key under the prefix is dropped, so the listing is the
	// one with no marker at all. The response still echoes the marker the client sent.
	if markerSortsBeforePrefix(prefix, marker) {
		marker = ""
	}
	prefixEndsOnDelimiter = strings.HasSuffix(prefix, "/") && len(marker) == 0
	// alignedDir should not end with "/"
	// alignedDir, alignedPrefix, alignedMarker should only have "/" in middle
	if len(marker) == 0 {
		prefix = strings.Trim(prefix, "/")
	} else {
		prefix = strings.TrimLeft(prefix, "/")
	}
	marker = strings.TrimLeft(marker, "/")
	if prefix == "" {
		return "", "", marker, prefixEndsOnDelimiter
	}
	if marker == "" {
		alignedDir, alignedPrefix = toDirAndName(prefix)
		return
	}
	if !strings.HasPrefix(marker, prefix) {
		// something wrong
		return "", prefix, marker, prefixEndsOnDelimiter
	}
	// Resolve the listing dir from the prefix, not the marker: a partial name prefix like
	// "data/a" also matches siblings such as "data/ab/", which narrowing to the marker's
	// subtree would drop.
	alignedDir, alignedPrefix = toDirAndName(prefix)
	if alignedDir != "" {
		alignedMarker = marker[len(alignedDir)+1:]
	} else {
		alignedMarker = marker
	}
	return
}

func toDirAndName(dirAndName string) (dir, name string) {
	sepIndex := strings.LastIndex(dirAndName, "/")
	if sepIndex >= 0 {
		dir, name = dirAndName[0:sepIndex], dirAndName[sepIndex+1:]
	} else {
		name = dirAndName
	}
	return
}

func toParentAndDescendants(dirAndName string) (dir, name string) {
	sepIndex := strings.Index(dirAndName, "/")
	if sepIndex >= 0 {
		dir, name = dirAndName[0:sepIndex], dirAndName[sepIndex+1:]
	} else {
		name = dirAndName
	}
	return
}

func buildTruncatedNextMarker(requestDir, nextMarker string, lastEntryWasCommonPrefix bool, lastCommonPrefix string) string {
	// The emitted CommonPrefix is already the full key path with its trailing delimiter.
	// Rebuilding it from requestDir plus the listing prefix breaks when that prefix is a
	// partial name, not a directory.
	if lastEntryWasCommonPrefix && lastCommonPrefix != "" {
		return lastCommonPrefix
	}

	if requestDir != "" {
		return requestDir + "/" + nextMarker
	}

	return nextMarker
}

type listDirectoryRequest struct {
	dir                string
	prefix             string
	marker             string
	delimiter          string
	bucket             string
	inclusiveStartFrom bool
}

func (s3a *S3ApiServer) doListFilerEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, req listDirectoryRequest, cursor *ListingCursor, eachEntryFn func(dir string, entry *filer_pb.Entry)) (nextMarker string, err error) {
	dir, prefix, bucket := req.dir, req.prefix, req.bucket
	marker, delimiter, inclusiveStartFrom := req.marker, req.delimiter, req.inclusiveStartFrom
	// invariants
	//   prefix and marker should be under dir, marker may contain "/"
	//   maxKeys should be updated for each recursion
	// glog.V(4).Infof("doListFilerEntries dir: %s, prefix: %s, marker %s, maxKeys: %d, prefixEndsOnDelimiter: %+v", dir, prefix, marker, cursor.maxKeys, cursor.prefixEndsOnDelimiter)
	// When listing at bucket root with delimiter '/', prefix can be "/" after normalization.
	// Returning early here would incorrectly hide all top-level entries (folders like "Veeam/").
	if cursor.maxKeys <= 0 {
		return // Don't set isTruncated here - let caller decide based on whether more entries exist
	}

	// A marker past the prefix's range leaves nothing under the prefix to
	// resume at, and descending into the marker's own directory below would
	// drop the prefix filter entirely.
	if markerSortsPastPrefix(prefix, marker) {
		return
	}

	// A directory's keys sort at "<name>/" among this level's children, not at
	// the name itself: siblings like "a-c" and "a.txt" sort before "a/b" while
	// "a0" sorts after. Start the window at the first child whose block can
	// still hold keys past the marker — the shortest prefix of the marker
	// followed by a byte no greater than "/" — and ask for it inclusively, so
	// a marker-named directory's subtree is reached at all.
	startName := marker
	for i := 0; i < len(marker); i++ {
		if marker[i] <= '/' {
			startName = marker[:i]
			break
		}
	}
	inclusiveStartFrom = startName != ""

	// now marker is also a direct child of dir
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The marker this page started from, unlike marker below, which advances with
	// each request window inside the page.
	pageMarker := marker

	// Defer each directory until a child past its "<name>/" block arrives, so
	// emission follows S3 byte order rather than the filer's name order.
	type pendingDir struct {
		e                *filer_pb.Entry
		matchedPrefixDir bool
	}
	var pendingDirs []pendingDir
	stopped := false
	// hasEntries reports whether the filer directory holds at least one entry,
	// probed on the request's own client; errors answer conservatively as true.
	hasEntries := func(path string) bool {
		probeCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, lerr := client.ListEntries(probeCtx, &filer_pb.ListEntriesRequest{Directory: path, Limit: 1})
		if lerr != nil {
			return true
		}
		_, recvErr := stream.Recv()
		return recvErr == nil
	}
	emitDir := func(e *filer_pb.Entry, matchedPrefixDir bool) error {
		blockKey := e.Name + "/"
		var subMarker string
		if strings.HasPrefix(marker, blockKey) {
			if delimiter == "/" {
				// this block collapses to one common prefix, already emitted
				return nil
			}
			subMarker = marker[len(blockKey):]
		} else if blockKey <= marker {
			return nil
		}
		// A prefix object's plain key is emitted at the name position or not at
		// all; the block emits only its "<name>/" fold, so the callback sees a
		// plain directory.
		emitEntry := e
		if e.IsPrefixObject() {
			emitEntry = proto.Clone(e).(*filer_pb.Entry)
			delete(emitEntry.Extended, s3_constants.SeaweedFSPrefixObject)
		}
		isKeyObject := e.IsDirectoryKeyObject() && !e.IsPrefixObject()
		if delimiter != "/" || cursor.prefixEndsOnDelimiter {
			// A trailing-slash prefix (e.g. "logs/") names one directory, so the
			// recursion below answers whether it exists and what it holds.
			if cursor.prefixEndsOnDelimiter {
				cursor.prefixEndsOnDelimiter = false
			}
			if isKeyObject && blockKey > marker {
				// Directory key objects (created via PutObject with trailing "/")
				// appear as regular keys in recursive listing mode.
				eachEntryFn(dir, emitEntry)
			}
			subNextMarker, subErr := s3a.doListFilerEntries(ctx, client, listDirectoryRequest{dir: dir + "/" + e.Name, marker: subMarker, delimiter: delimiter, bucket: bucket}, cursor, eachEntryFn)
			if subErr != nil {
				return fmt.Errorf("doListFilerEntries2: %w", subErr)
			}
			nextMarker = e.Name + "/" + subNextMarker
			if cursor.maxKeys <= 0 && subNextMarker == "" && hasEntries(dir+"/"+e.Name) {
				// The recursion starved on quota before listing the subtree;
				// keep a resume position that re-enters it.
				cursor.isTruncated = true
			}
			// An empty directory gets no marker under its own "<dir>/" prefix:
			// that listing enumerates children, where S3 returns nothing for an
			// empty prefix, and a fake entry is read back as a real object by
			// clients that enumerate locks or similar.
			// println("doListFilerEntries2 dir", dir+"/"+e.Name, "subNextMarker", subNextMarker)
			if cursor.isTruncated || matchedPrefixDir {
				stopped = true
			}
			// println("doListFilerEntries2 nextMarker", nextMarker)
		} else {
			switch {
			case isKeyObject && blockKey > marker:
				eachEntryFn(dir, emitEntry)
			case e.IsPrefixObject():
				// The plain key already listed at the name position; the
				// deferred block emits the "<name>/" common prefix only when
				// children fold under it.
				if hasEntries(dir + "/" + e.Name) {
					eachEntryFn(dir, emitEntry)
				}
			case !s3a.dirHoldsOnlyHiddenEntries(ctx, client, bucket, dir+"/"+e.Name, cursor):
				eachEntryFn(dir, emitEntry)
			}
			// Whatever the block emitted sits at its "<name>/" position, so the
			// resume marker is the block boundary: the bare name sorts inside the
			// (name, name/) band and would reopen the block for a second emission.
			nextMarker = e.Name + "/"
		}
		return nil
	}
	// pendingDirs is kept ordered by "<name>/" rather than by name: "a-c/" and
	// "a./" sort before "a/" even though "a" is the smaller name.
	flushDirs := func(upTo string) error {
		for len(pendingDirs) > 0 && (upTo == "" || pendingDirs[0].e.Name+"/" < upTo) {
			if cursor.maxKeys <= 0 {
				cursor.isTruncated = true
				return nil
			}
			d := pendingDirs[0]
			pendingDirs = pendingDirs[1:]
			if err := emitDir(d.e, d.matchedPrefixDir); err != nil {
				return err
			}
			if stopped || cursor.isTruncated {
				return nil
			}
		}
		return nil
	}
	queueDir := func(pd pendingDir) {
		blockKey := pd.e.Name + "/"
		i := 0
		for i < len(pendingDirs) && pendingDirs[i].e.Name+"/" < blockKey {
			i++
		}
		pendingDirs = append(pendingDirs, pendingDir{})
		copy(pendingDirs[i+1:], pendingDirs[i:])
		pendingDirs[i] = pd
	}

	// Entries that emit nothing (empty directories, the .uploads folder, the marker
	// echo) consume the request window without consuming maxKeys, so one window may
	// end before maxKeys is satisfied. Keep requesting from the last received entry
	// until the quota is filled or a short window shows the directory is exhausted.
	for {
		request := &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             prefix,
			Limit:              uint32(cursor.maxKeys) + 2, // bucket root directory needs to skip additional s3_constants.MultipartUploadsFolder folder
			StartFromFileName:  startName,
			InclusiveStartFrom: inclusiveStartFrom,
		}

		stream, listErr := client.ListEntries(ctx, request)
		if listErr != nil {
			if errors.Is(listErr, filer_pb.ErrNotFound) {
				return
			}
			err = fmt.Errorf("list entries %+v: %w", request, listErr)
			return
		}

		var entriesReceived uint32
		var lastEntryName string
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					err = fmt.Errorf("iterating entries %+v: %v", request, recvErr)
					return
				}
			}
			entry := resp.Entry
			if entry == nil {
				continue
			}
			entriesReceived++
			lastEntryName = entry.Name
			// A directory whose block sorts before this entry emits now — every
			// key it yields still sorts before this entry's name.
			if err = flushDirs(entry.Name); err != nil || stopped || cursor.isTruncated {
				return
			}
			// A directory block still contributes past a marker that lands inside
			// its "<name>/" band; only a block entirely behind it is skipped.
			if marker != "" {
				if entry.IsDirectory {
					if entry.Name+"/" <= marker && !strings.HasPrefix(marker, entry.Name+"/") {
						continue
					}
				} else if entry.Name <= marker {
					continue
				}
			}

			// The .versions sibling of the key just emitted still decides that key's
			// fate (metadata replacement or retraction) and consumes no quota of its
			// own, so it must not be pushed past the page boundary.
			versionsSiblingOfLast := cursor.hideDeletedPrefixes && entry.IsDirectory &&
				entry.Name == nextMarker+s3_constants.VersionsFolder
			if cursor.maxKeys <= 0 && !versionsSiblingOfLast {
				if cursor.resolvePendingNulls != nil {
					if err = cursor.resolvePendingNulls(); err != nil {
						return
					}
				}
				if cursor.maxKeys <= 0 {
					cursor.isTruncated = true
					break
				}
			}

			// Set nextMarker only when we have quota to process this entry
			nextMarker = entry.Name
			// Track whether this entry is the exact directory targeted by a trailing-slash prefix
			// (e.g., prefix "foo" from original prefix "foo/"). After recursing into this directory,
			// we must stop processing siblings to avoid matching unrelated entries like "foo1000".
			matchedPrefixDir := cursor.prefixEndsOnDelimiter && entry.Name == prefix && entry.IsDirectory
			if cursor.prefixEndsOnDelimiter {
				if !matchedPrefixDir {
					continue
				}
				// Keep the exact-directory filter until emitDir descends into
				// the deferred block. Clearing it at the name position lets
				// siblings like "a.txt" leak into a listing for "a/".
			}
			if entry.IsDirectory {
				// glog.V(4).Infof("List Dir Entries %s, file: %s, maxKeys %d", dir, entry.Name, cursor.maxKeys)
				// A marker inside a skipped subtree still descends — the
				// internal folder is not listed, its contents may be.
				if strings.HasPrefix(marker, entry.Name+"/") {
					if delimiter == "/" {
						// The whole block already collapsed into its common
						// prefix; nothing under it is listed again.
						continue
					}
					subNextMarker, subErr := s3a.doListFilerEntries(ctx, client, listDirectoryRequest{dir: dir + "/" + entry.Name, marker: marker[len(entry.Name)+1:], delimiter: delimiter, bucket: bucket}, cursor, eachEntryFn)
					if subErr != nil {
						err = fmt.Errorf("doListFilerEntries: %w", subErr)
						return
					}
					nextMarker = entry.Name + "/" + subNextMarker
					if cursor.isTruncated {
						return
					}
					continue
				}
				if entry.Name == s3_constants.MultipartUploadsFolder { // FIXME no need to apply to all directories. this extra also affects maxKeys
					continue
				}

				// Process .versions directories immediately to create logical versioned object entries
				// These directories are never traversed (we continue here), so each is only encountered once
				if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
					if entry.Name == s3_constants.VersionsFolder {
						// The history of the key "<dir>/", not of a child. The parent
						// listing decides that key when it reaches the directory entry.
						continue
					}
					// Extract object name from .versions directory name
					baseObjectName := strings.TrimSuffix(entry.Name, s3_constants.VersionsFolder)
					// A marker at or past the folded key already covered it.
					if marker != "" && baseObjectName <= marker {
						continue
					}
					// A page resuming from a marker inside the base key's extension
					// region ("k.bak" sorts between "k" and "k.versions") means an
					// earlier page already listed and settled the base null object;
					// resolving this directory again would duplicate the key. Without
					// a base object the key has not been listed yet, so it still
					// resolves here.
					if pageMarker != "" && baseObjectName < pageMarker {
						if _, baseErr := s3a.getEntry(dir, baseObjectName); baseErr == nil {
							continue
						}
					}
					// Construct full object path relative to bucket
					bucketFullPath := s3a.bucketDir(bucket)
					bucketRelativePath := strings.TrimPrefix(dir, bucketFullPath)
					bucketRelativePath = strings.TrimPrefix(bucketRelativePath, "/")
					var fullObjectPath string
					if bucketRelativePath == "" {
						fullObjectPath = baseObjectName
					} else {
						fullObjectPath = bucketRelativePath + "/" + baseObjectName
					}
					// Use metadata from the already-fetched .versions directory entry
					if latestVersionEntry, err := s3a.getLatestVersionEntryFromDirectoryEntry(bucket, fullObjectPath, entry); err == nil {
						eachEntryFn(dir, latestVersionEntry)
					} else if errors.Is(err, ErrDeleteMarker) {
						// The current version is a delete marker, so a null object listed
						// for the base path just before this directory is stale.
						if cursor.retractEntry != nil {
							cursor.retractEntry(dir, baseObjectName)
						}
					} else {
						glog.V(2).Infof("Skipping versioned object %s due to error: %v", fullObjectPath, err)
					}
					continue
				}

				pd := pendingDir{e: entry, matchedPrefixDir: matchedPrefixDir}
				// A prefix object lists as the plain key "<name>", which sorts
				// at the name, not at the "<name>/" block its children occupy; the
				// deferred block emits the common prefix at its own position.
				if entry.IsPrefixObject() && entry.Name > marker {
					cursor.prefixObjectKeyOnly = true
					eachEntryFn(dir, entry)
				}
				queueDir(pd)
				continue
			}
			eachEntryFn(dir, entry)
			// glog.V(4).Infof("List File Entries %s, file: %s, maxKeys %d", dir, entry.Name, cursor.maxKeys)
			if cursor.prefixEndsOnDelimiter {
				cursor.prefixEndsOnDelimiter = false
			}
		}

		if cursor.isTruncated || stopped {
			return
		}
		if entriesReceived < request.Limit {
			err = flushDirs("")
			return
		}
		startName = lastEntryName
		inclusiveStartFrom = false
	}
}

// hiddenProbePageSize is the window one probe request asks the filer for.
// hiddenProbeBudget caps how many entries one prefix's probe may scan; it
// resets for every candidate prefix so one large all-deleted subtree cannot
// exhaust the allowance for later prefixes. hiddenProbeTotalBudget caps the
// scanned entries a single listing request spends on probing overall, since a
// page may still walk an unbounded number of all-deleted prefixes.
const (
	hiddenProbePageSize    = 64
	hiddenProbeBudget      = 10000
	hiddenProbeTotalBudget = 100000
)

// dirHoldsOnlyHiddenEntries reports whether dir holds entries but none that a
// current-version listing returns. Deleting the last object under a prefix in a
// versioned bucket leaves the version history and a delete marker behind, so the filer
// directory survives with nothing listable in it. AWS derives CommonPrefixes from the
// keys a listing returns, so that path is no longer a prefix and no longer a directory
// to answer a probe for. An empty directory is left alone: mount and mkdir create them
// and empty-folder cleanup owns their lifetime.
//
// The scan stops at the first key it finds, so a populated prefix costs one ListEntries
// answered by its first entry. A subtree that is entirely delete-marked costs a walk of
// that subtree, bounded by hiddenProbeBudget entries per prefix. The budget resets for
// each candidate prefix, so a large all-deleted subtree cannot exhaust the allowance
// for later prefixes in the same request.
func (s3a *S3ApiServer) dirHoldsOnlyHiddenEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, bucket, dir string, cursor *ListingCursor) bool {
	if !cursor.hideDeletedPrefixes {
		return false
	}
	budget := hiddenProbeBudget
	if cursor.probeBudget > 0 {
		budget = cursor.probeBudget
	}
	totalBudget := hiddenProbeTotalBudget
	if cursor.probeTotalBudget > 0 {
		totalBudget = cursor.probeTotalBudget
	}
	return s3a.dirHoldsOnlyHiddenEntriesInner(ctx, client, bucket, dir, &budget, totalBudget, cursor)
}

// dirHoldsOnlyHiddenEntriesInner is the recursive body of dirHoldsOnlyHiddenEntries.
// budget is shared across the recursive descent for one prefix but reset by the
// caller for each top-level prefix, preventing cross-prefix budget exhaustion.
// cursor.probedEntries bounds the total work one listing request spends probing.
func (s3a *S3ApiServer) dirHoldsOnlyHiddenEntriesInner(ctx context.Context, client filer_pb.SeaweedFilerClient, bucket, dir string, budget *int, totalBudget int, cursor *ListingCursor) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sawEntry := false
	startFrom := ""
	// A plain file is a null object that its .versions sibling, streaming later,
	// may prove delete-marked; it stays pending until then. A pending file whose
	// sibling window closes without one is a live key.
	var pendingFiles []string
	for {
		request := &filer_pb.ListEntriesRequest{
			Directory:         dir,
			StartFromFileName: startFrom,
			Limit:             hiddenProbePageSize,
		}
		stream, listErr := client.ListEntries(ctx, request)
		if listErr != nil {
			if !errors.Is(listErr, filer_pb.ErrNotFound) {
				glog.V(1).Infof("dirHoldsOnlyHiddenEntries %s: %v", dir, listErr)
			}
			return false
		}

		var entriesReceived uint32
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr != io.EOF {
					glog.V(1).Infof("dirHoldsOnlyHiddenEntries %s: %v", dir, recvErr)
					return false
				}
				break
			}
			entry := resp.Entry
			if entry == nil {
				continue
			}
			entriesReceived++
			startFrom = entry.Name
			sawEntry = true

			*budget--
			cursor.probedEntries++
			if *budget < 0 || cursor.probedEntries > totalBudget {
				return false
			}

			for _, pendingFile := range pendingFiles {
				if entry.Name > pendingFile+s3_constants.VersionsFolder {
					return false
				}
			}
			if !entry.IsDirectory {
				pendingFiles = append(pendingFiles, entry.Name)
				continue
			}
			if entry.Name == s3_constants.MultipartUploadsFolder {
				continue
			}
			if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
				// Each write that changes an object's current version stamps the answer
				// onto its .versions directory entry, which the listing above already
				// carries, so a delete-marked object costs nothing to recognize. A
				// missing stamp leaves the current version unknown - the pointer is
				// written on the key's owner filer and may not have reached the filer
				// serving this list - and an unknown object keeps its prefix rather than
				// turning one listing into a version rescan per object.
				if isDeleteMarker, stamped := entry.Extended[s3_constants.ExtLatestVersionIsDeleteMarker]; stamped && string(isDeleteMarker) == "true" {
					// The marker also hides the null object the key left at the base path.
					base := strings.TrimSuffix(entry.Name, s3_constants.VersionsFolder)
					kept := pendingFiles[:0]
					for _, pendingFile := range pendingFiles {
						if pendingFile != base {
							kept = append(kept, pendingFile)
						}
					}
					pendingFiles = kept
					continue
				}
				return false
			}
			if entry.IsDirectoryKeyObject() {
				return false
			}
			if !s3a.dirHoldsOnlyHiddenEntriesInner(ctx, client, bucket, dir+"/"+entry.Name, budget, totalBudget, cursor) {
				return false
			}
		}

		if entriesReceived < request.Limit {
			return sawEntry && len(pendingFiles) == 0
		}
	}
}

func getListObjectsV2Args(values url.Values) (prefix, startAfter, delimiter string, token OptionalString, encodingTypeUrl bool, fetchOwner bool, maxkeys uint16, allowUnordered bool, errCode s3err.ErrorCode) {
	prefix = values.Get("prefix")
	token = OptionalString{set: values.Has("continuation-token"), string: values.Get("continuation-token")}
	startAfter = values.Get("start-after")
	delimiter = values.Get("delimiter")
	encodingTypeUrl = values.Get("encoding-type") == s3.EncodingTypeUrl
	maxkeys, errCode = parseMaxKeys(values.Get("max-keys"))
	if errCode != s3err.ErrNone {
		return
	}
	fetchOwner = values.Get("fetch-owner") == "true"
	allowUnordered = values.Get("allow-unordered") == "true"
	errCode = s3err.ErrNone
	return
}

func getListObjectsV1Args(values url.Values) (prefix, marker, delimiter string, encodingTypeUrl bool, maxkeys uint16, allowUnordered bool, errCode s3err.ErrorCode) {
	prefix = values.Get("prefix")
	marker = values.Get("marker")
	delimiter = values.Get("delimiter")
	encodingTypeUrl = values.Get("encoding-type") == "url"
	maxkeys, errCode = parseMaxKeys(values.Get("max-keys"))
	if errCode != s3err.ErrNone {
		return
	}
	allowUnordered = values.Get("allow-unordered") == "true"
	errCode = s3err.ErrNone
	return
}

// parseMaxKeys parses the max-keys query parameter. Like AWS S3, any integer in
// [0, 2147483647] is accepted and the page size is capped at maxObjectListSizeLimit;
// an empty value means the default limit, anything else is ErrInvalidMaxKeys.
func parseMaxKeys(value string) (uint16, s3err.ErrorCode) {
	if value == "" {
		return maxObjectListSizeLimit, s3err.ErrNone
	}
	maxKeys, err := strconv.ParseInt(value, 10, 32)
	if err != nil || maxKeys < 0 {
		return 0, s3err.ErrInvalidMaxKeys
	}
	if maxKeys > maxObjectListSizeLimit {
		return maxObjectListSizeLimit, s3err.ErrNone
	}
	return uint16(maxKeys), s3err.ErrNone
}

// compareWithDelimiter compares two strings for sorting, treating the delimiter character
// as having lower precedence than other characters to match AWS S3 behavior.
// For example, with delimiter '/', 'foo/' should come before 'foo+1/' even though '+' < '/' in ASCII.
// Note: This function assumes delimiter is a single character. Multi-character delimiters will fall back to standard comparison.
func compareWithDelimiter(a, b, delimiter string) bool {
	if delimiter == "" {
		return a < b
	}

	// Multi-character delimiters are not supported by AWS S3 in practice,
	// but if encountered, fall back to standard byte-wise comparison
	if len(delimiter) != 1 {
		return a < b
	}

	delimByte := delimiter[0]
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	// Compare character by character
	for i := 0; i < minLen; i++ {
		charA := a[i]
		charB := b[i]

		if charA == charB {
			continue
		}

		// Check if either character is the delimiter
		isDelimA := charA == delimByte
		isDelimB := charB == delimByte

		if isDelimA && !isDelimB {
			// Delimiter in 'a' should come first
			return true
		}
		if !isDelimA && isDelimB {
			// Delimiter in 'b' should come first
			return false
		}

		// Neither or both are delimiters, use normal comparison
		return charA < charB
	}

	// If we get here, one string is a prefix of the other
	return len(a) < len(b)
}

// adjustMarkerForDelimiter handles delimiter-ending markers by incrementing them to skip entries with that prefix.
// For example, when continuation token is "boo/", this returns "boo~" to skip all "boo/*" entries
// but still finds any "bop" or later entries. We add a high ASCII character rather than incrementing
// the last character to avoid skipping potential directory entries.
// This is essential for correct S3 list operations with delimiters and CommonPrefixes.
// A marker equal to the prefix names no subtree to skip: it excludes only the prefix's own
// key. Leading slashes are insignificant here, as they are to normalizePrefixMarker.
func adjustMarkerForDelimiter(marker, prefix, delimiter string) string {
	if delimiter == "" || !strings.HasSuffix(marker, delimiter) {
		return marker
	}
	if strings.TrimLeft(marker, "/") == strings.TrimLeft(prefix, "/") {
		return marker
	}

	// A marker ending on "/" already names the bound past that directory's
	// block — the walk reads "boo/" as "the boo/ block is done". Trimming it
	// to the bare name would sort inside the (name, name/) band instead and
	// reopen the block. Other delimiters have no matching directory boundary,
	// so their markers still need the trim below.
	if delimiter == "/" {
		return marker
	}

	// Remove the trailing delimiter
	// This ensures we skip all entries under the prefix but don't skip
	// potential directory entries that start with a similar prefix
	trimmed := strings.TrimSuffix(marker, delimiter)
	if len(trimmed) == 0 {
		return marker
	}

	return trimmed
}
