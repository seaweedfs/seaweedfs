package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postTagAction issues a form-encoded IAM request via the test router.
func postTagAction(t *testing.T, api *EmbeddedIamApiForTest, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.PostForm = form
	req.Form = form
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r := mux.NewRouter().SkipClean(true)
	r.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
	r.ServeHTTP(rr, req)
	return rr
}

func findIdentity(cfg *iam_pb.S3ApiConfiguration, name string) *iam_pb.Identity {
	for _, ident := range cfg.Identities {
		if ident.Name == name {
			return ident
		}
	}
	return nil
}

func TestEmbeddedIamTagUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	}

	params := &iam.TagUserInput{
		UserName: aws.String("alice"),
		Tags: []*iam.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("storage")},
		},
	}
	req, _ := iam.New(session.New()).TagUserRequest(params)
	require.NoError(t, req.Build())

	out := iamTagUserResponse{}
	rr, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rr.Code)

	ident := findIdentity(api.mockConfig, "alice")
	require.NotNil(t, ident)
	require.Len(t, ident.Tags, 2)
	assert.Equal(t, "env", ident.Tags[0].Key)
	assert.Equal(t, "prod", ident.Tags[0].Value)
	assert.Equal(t, "team", ident.Tags[1].Key)
	assert.Equal(t, "storage", ident.Tags[1].Value)
}

func TestEmbeddedIamListUserTags(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "bob",
				Tags: []*iam_pb.UserTag{
					{Key: "env", Value: "stage"},
					{Key: "owner", Value: "bob"},
				},
			},
		},
	}

	params := &iam.ListUserTagsInput{UserName: aws.String("bob")}
	req, _ := iam.New(session.New()).ListUserTagsRequest(params)
	require.NoError(t, req.Build())

	out := iamListUserTagsResponse{}
	rr, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rr.Code)

	require.Len(t, out.ListUserTagsResult.Tags, 2)
	assert.Equal(t, "env", out.ListUserTagsResult.Tags[0].Key)
	assert.Equal(t, "stage", out.ListUserTagsResult.Tags[0].Value)
	assert.False(t, out.ListUserTagsResult.IsTruncated)
}

func TestEmbeddedIamUntagUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "carol",
				Tags: []*iam_pb.UserTag{
					{Key: "env", Value: "prod"},
					{Key: "team", Value: "storage"},
					{Key: "owner", Value: "carol"},
				},
			},
		},
	}

	params := &iam.UntagUserInput{
		UserName: aws.String("carol"),
		TagKeys:  []*string{aws.String("team"), aws.String("missing")},
	}
	req, _ := iam.New(session.New()).UntagUserRequest(params)
	require.NoError(t, req.Build())

	rr, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rr.Code)

	ident := findIdentity(api.mockConfig, "carol")
	require.NotNil(t, ident)
	require.Len(t, ident.Tags, 2)
	keys := []string{ident.Tags[0].Key, ident.Tags[1].Key}
	assert.ElementsMatch(t, []string{"env", "owner"}, keys)
}

func TestEmbeddedIamTagUserInvalidKeyLength(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "dave"}},
	}

	longKey := strings.Repeat("k", MaxUserTagKeyLength+1)
	params := &iam.TagUserInput{
		UserName: aws.String("dave"),
		Tags:     []*iam.Tag{{Key: aws.String(longKey), Value: aws.String("v")}},
	}
	req, _ := iam.New(session.New()).TagUserRequest(params)
	require.NoError(t, req.Build())

	rr, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.NoError(t, err)
	assert.NotEqual(t, http.StatusOK, rr.Code)
	code, _ := extractEmbeddedIamErrorCodeAndMessage(rr)
	assert.Equal(t, "ValidationError", code)
}

func TestEmbeddedIamTagUserReplacesDuplicate(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "eve",
				Tags: []*iam_pb.UserTag{{Key: "env", Value: "stage"}},
			},
		},
	}

	params := &iam.TagUserInput{
		UserName: aws.String("eve"),
		Tags:     []*iam.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	}
	req, _ := iam.New(session.New()).TagUserRequest(params)
	require.NoError(t, req.Build())

	rr, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rr.Code)

	ident := findIdentity(api.mockConfig, "eve")
	require.NotNil(t, ident)
	require.Len(t, ident.Tags, 1)
	assert.Equal(t, "env", ident.Tags[0].Key)
	assert.Equal(t, "prod", ident.Tags[0].Value)
}

func TestEmbeddedIamTagUserLimitExceeded(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	existing := make([]*iam_pb.UserTag, 0, MaxUserTags)
	for i := 0; i < MaxUserTags; i++ {
		existing = append(existing, &iam_pb.UserTag{Key: fmt.Sprintf("k%02d", i), Value: "v"})
	}
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "frank", Tags: existing}},
	}

	form := url.Values{}
	form.Set("Action", "TagUser")
	form.Set("UserName", "frank")
	form.Set("Tags.member.1.Key", "extra")
	form.Set("Tags.member.1.Value", "v")
	rr := postTagAction(t, api, form)

	assert.Equal(t, http.StatusForbidden, rr.Code)
	code, _ := extractEmbeddedIamErrorCodeAndMessage(rr)
	assert.Equal(t, iam.ErrCodeLimitExceededException, code)

	ident := findIdentity(api.mockConfig, "frank")
	require.NotNil(t, ident)
	assert.Len(t, ident.Tags, MaxUserTags)
}

func TestEmbeddedIamUntagUserNoOpForMissingKey(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "grace",
				Tags: []*iam_pb.UserTag{{Key: "env", Value: "prod"}},
			},
		},
	}

	form := url.Values{}
	form.Set("Action", "UntagUser")
	form.Set("UserName", "grace")
	form.Set("TagKeys.member.1", "missing")
	rr := postTagAction(t, api, form)

	assert.Equal(t, http.StatusOK, rr.Code)
	ident := findIdentity(api.mockConfig, "grace")
	require.NotNil(t, ident)
	require.Len(t, ident.Tags, 1)
	assert.Equal(t, "env", ident.Tags[0].Key)
}

func TestEmbeddedIamTagUserNotFound(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	params := &iam.TagUserInput{
		UserName: aws.String("ghost"),
		Tags:     []*iam.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	}
	req, _ := iam.New(session.New()).TagUserRequest(params)
	require.NoError(t, req.Build())

	rr, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rr.Code)
	code, _ := extractEmbeddedIamErrorCodeAndMessage(rr)
	assert.Equal(t, iam.ErrCodeNoSuchEntityException, code)
}
