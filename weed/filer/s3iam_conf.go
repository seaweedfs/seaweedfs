package filer

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/protobuf/proto"
)

const (
	S3IamConfigDirectory   = "/etc/s3"
	S3IamConfigFile        = "s3_config.json"
	IamPoliciesDirectory   = "/iam/policies"
	IamRolesDirectory      = "/iam/roles"
	IamIdentitiesDirectory = "/iam/identities"
	IamUsersDirectory      = "/iam/users"
)

func ParseS3ConfigurationFromBytes(content []byte, config proto.Message) error {
	if err := proto.Unmarshal(content, config); err != nil {
		return err
	}
	return nil
}

func ProtoToText(writer io.Writer, msg proto.Message) error {
	text, marshalErr := proto.Marshal(msg)
	if marshalErr != nil {
		return fmt.Errorf("marshal proto message: %w", marshalErr)
	}

	_, writeErr := writer.Write(text)
	if writeErr != nil {
		return fmt.Errorf("fail to write proto message: %w", writeErr)
	}

	return writeErr
}

// S3ApiConfigurationToText - renamed to avoid conflict
func S3ApiConfigurationToText(config *iam_pb.S3ApiConfiguration) ([]byte, error) {
	text, marshalErr := proto.Marshal(config)
	if marshalErr != nil {
		return nil, fmt.Errorf("marshal proto message: %w", marshalErr)
	}
	return text, nil
}

// CheckDuplicateAccessKey returns an error message when s3cfg has duplicate access keys
func CheckDuplicateAccessKey(s3cfg *iam_pb.S3ApiConfiguration) error {
	accessKeySet := make(map[string]string)
	for _, ident := range s3cfg.Identities {
		for _, cred := range ident.Credentials {
			if userName, found := accessKeySet[cred.AccessKey]; !found {
				accessKeySet[cred.AccessKey] = ident.Name
			} else if userName != ident.Name {
				return fmt.Errorf("duplicate accessKey[%s], already configured in user[%s]", cred.AccessKey, userName)
			}
		}
	}
	return nil
}
