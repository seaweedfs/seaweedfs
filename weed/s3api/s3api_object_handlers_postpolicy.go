package s3api

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func (s3a *S3ApiServer) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html

	bucket := mux.Vars(r)["bucket"]

	glog.V(3).Infof("PostPolicyBucketHandler %s", bucket)

	reader, err := r.MultipartReader()
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedPOSTRequest)
		return
	}
	form, err := reader.ReadForm(int64(5 * humanize.MiByte))
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedPOSTRequest)
		return
	}
	defer form.RemoveAll()

	fileBody, fileName, fileContentType, fileSize, formValues, err := extractPostPolicyFormValues(form)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedPOSTRequest)
		return
	}
	if fileBody == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrPOSTFileRequired)
		return
	}
	defer fileBody.Close()

	formValues.Set("Bucket", bucket)

	if fileName != "" && strings.Contains(formValues.Get("Key"), "${filename}") {
		formValues.Set("Key", strings.Replace(formValues.Get("Key"), "${filename}", fileName, -1))
	}
	object := formValues.Get("Key")

	successRedirect := formValues.Get("success_action_redirect")
	successStatus := formValues.Get("success_action_status")
	var redirectURL *url.URL
	if successRedirect != "" {
		redirectURL, err = url.Parse(successRedirect)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrMalformedPOSTRequest)
			return
		}
	}

	// Verify policy signature.
	errCode := s3a.iam.doesPolicySignatureMatch(formValues)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues.Get("Policy"))
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedPOSTRequest)
		return
	}

	// Handle policy if it is set.
	if len(policyBytes) > 0 {

		postPolicyForm, err := policy.ParsePostPolicyForm(string(policyBytes))
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrPostPolicyConditionInvalidFormat)
			return
		}

		// Make sure formValues adhere to policy restrictions.
		if err = policy.CheckPostPolicy(formValues, postPolicyForm); err != nil {
			w.Header().Set("Location", r.URL.Path)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}

		// Ensure that the object size is within expected range, also the file size
		// should not exceed the maximum single Put size (5 GiB)
		lengthRange := postPolicyForm.Conditions.ContentLengthRange
		if lengthRange.Valid {
			if fileSize < lengthRange.Min {
				s3err.WriteErrorResponse(w, r, s3err.ErrEntityTooSmall)
				return
			}

			if fileSize > lengthRange.Max {
				s3err.WriteErrorResponse(w, r, s3err.ErrEntityTooLarge)
				return
			}
		}
	}

	uploadUrl := fmt.Sprintf("http://%s%s/%s%s", s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, bucket, urlEscapeObject(object))

	// Get ContentType from post formData
	// Otherwise from formFile ContentType
	contentType := formValues.Get("Content-Type")
	if contentType == "" {
		contentType = fileContentType
	}
	r.Header.Set("Content-Type", contentType)

	// Add s3 postpolicy support header
	for k, _ := range formValues {
		if k == "Cache-Control" || k == "Expires" || k == "Content-Disposition" {
			r.Header.Set(k, formValues.Get(k))
			continue
		}

		if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
			r.Header.Set(k, formValues.Get(k))
		}
	}

	etag, errCode := s3a.putToFiler(r, uploadUrl, fileBody, "", bucket)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if successRedirect != "" {
		// Replace raw query params..
		redirectURL.RawQuery = getRedirectPostRawQuery(bucket, object, etag)
		w.Header().Set("Location", redirectURL.String())
		s3err.WriteEmptyResponse(w, r, http.StatusSeeOther)
		return
	}

	setEtag(w, etag)

	// Decide what http response to send depending on success_action_status parameter
	switch successStatus {
	case "201":
		resp := PostResponse{
			Bucket:   bucket,
			Key:      object,
			ETag:     `"` + etag + `"`,
			Location: w.Header().Get("Location"),
		}
		s3err.WriteXMLResponse(w, r, http.StatusCreated, resp)
		s3err.PostLog(r, http.StatusCreated, s3err.ErrNone)
	case "200":
		s3err.WriteEmptyResponse(w, r, http.StatusOK)
	case "204":
		s3err.WriteEmptyResponse(w, r, http.StatusNoContent)
	default:
		s3err.WriteEmptyResponse(w, r, http.StatusNoContent)
	}

}

// Extract form fields and file data from a HTTP POST Policy
func extractPostPolicyFormValues(form *multipart.Form) (filePart io.ReadCloser, fileName, fileContentType string, fileSize int64, formValues http.Header, err error) {
	// / HTML Form values
	fileName = ""
	fileContentType = ""

	// Canonicalize the form values into http.Header.
	formValues = make(http.Header)
	for k, v := range form.Value {
		formValues[http.CanonicalHeaderKey(k)] = v
	}

	// Validate form values.
	if err = validateFormFieldSize(formValues); err != nil {
		return nil, "", "", 0, nil, err
	}

	// this means that filename="" was not specified for file key and Go has
	// an ugly way of handling this situation. Refer here
	// https://golang.org/src/mime/multipart/formdata.go#L61
	if len(form.File) == 0 {
		var b = &bytes.Buffer{}
		for _, v := range formValues["File"] {
			b.WriteString(v)
		}
		fileSize = int64(b.Len())
		filePart = io.NopCloser(b)
		return filePart, fileName, fileContentType, fileSize, formValues, nil
	}

	// Iterator until we find a valid File field and break
	for k, v := range form.File {
		canonicalFormName := http.CanonicalHeaderKey(k)
		if canonicalFormName == "File" {
			if len(v) == 0 {
				return nil, "", "", 0, nil, errors.New("Invalid arguments specified")
			}
			// Fetch fileHeader which has the uploaded file information
			fileHeader := v[0]
			// Set filename
			fileName = fileHeader.Filename
			// Set contentType
			fileContentType = fileHeader.Header.Get("Content-Type")
			// Open the uploaded part
			filePart, err = fileHeader.Open()
			if err != nil {
				return nil, "", "", 0, nil, err
			}
			// Compute file size
			fileSize, err = filePart.(io.Seeker).Seek(0, 2)
			if err != nil {
				return nil, "", "", 0, nil, err
			}
			// Reset Seek to the beginning
			_, err = filePart.(io.Seeker).Seek(0, 0)
			if err != nil {
				return nil, "", "", 0, nil, err
			}
			// File found and ready for reading
			break
		}
	}
	return filePart, fileName, fileContentType, fileSize, formValues, nil
}

// Validate form field size for s3 specification requirement.
func validateFormFieldSize(formValues http.Header) error {
	// Iterate over form values
	for k := range formValues {
		// Check if value's field exceeds S3 limit
		if int64(len(formValues.Get(k))) > int64(1*humanize.MiByte) {
			return errors.New("Data size larger than expected")
		}
	}

	// Success.
	return nil
}

func getRedirectPostRawQuery(bucket, key, etag string) string {
	redirectValues := make(url.Values)
	redirectValues.Set("bucket", bucket)
	redirectValues.Set("key", key)
	redirectValues.Set("etag", "\""+etag+"\"")
	return redirectValues.Encode()
}

// Check to see if Policy is signed correctly.
func (iam *IdentityAccessManagement) doesPolicySignatureMatch(formValues http.Header) s3err.ErrorCode {
	// For SignV2 - Signature field will be valid
	if _, ok := formValues["Signature"]; ok {
		return iam.doesPolicySignatureV2Match(formValues)
	}
	return iam.doesPolicySignatureV4Match(formValues)
}
