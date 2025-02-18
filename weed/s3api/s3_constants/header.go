/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package s3_constants

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// Standard S3 HTTP request constants
const (
	// S3 storage class
	AmzStorageClass = "x-amz-storage-class"

	// S3 user-defined metadata
	AmzUserMetaPrefix    = "X-Amz-Meta-"
	AmzUserMetaDirective = "X-Amz-Metadata-Directive"
	AmzUserMetaMtime     = "X-Amz-Meta-Mtime"

	// S3 object tagging
	AmzObjectTagging          = "X-Amz-Tagging"
	AmzObjectTaggingPrefix    = "X-Amz-Tagging-"
	AmzObjectTaggingDirective = "X-Amz-Tagging-Directive"
	AmzTagCount               = "x-amz-tagging-count"

	SeaweedFSIsDirectoryKey = "X-Seaweedfs-Is-Directory-Key"
	SeaweedFSPartNumber     = "X-Seaweedfs-Part-Number"
	SeaweedFSUploadId       = "X-Seaweedfs-Upload-Id"

	// S3 ACL headers
	AmzCannedAcl      = "X-Amz-Acl"
	AmzAclFullControl = "X-Amz-Grant-Full-Control"
	AmzAclRead        = "X-Amz-Grant-Read"
	AmzAclWrite       = "X-Amz-Grant-Write"
	AmzAclReadAcp     = "X-Amz-Grant-Read-Acp"
	AmzAclWriteAcp    = "X-Amz-Grant-Write-Acp"

	AmzMpPartsCount = "X-Amz-Mp-Parts-Count"
)

// Non-Standard S3 HTTP request constants
const (
	AmzIdentityId = "s3-identity-id"
	AmzAccountId  = "s3-account-id"
	AmzAuthType   = "s3-auth-type"
	AmzIsAdmin    = "s3-is-admin" // only set to http request header as a context
)

func GetBucketAndObject(r *http.Request) (bucket, object string) {
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]
	if !strings.HasPrefix(object, "/") {
		object = "/" + object
	}

	return
}

func GetPrefix(r *http.Request) string {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	return prefix
}

var PassThroughHeaders = map[string]string{
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
	"response-content-language":    "Content-Language",
	"response-content-type":        "Content-Type",
	"response-expires":             "Expires",
}
