package s3iam

import (
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"time"

	proto "github.com/golang/protobuf/proto"
)

const (
	iamConfigPrefix = "/etc/iam"
	iamIdentityFile = "identity.json"
)

type IAMFilerStore struct {
	client *filer_pb.SeaweedFilerClient
}

func NewIAMFilerStore(client *filer_pb.SeaweedFilerClient) *IAMFilerStore {
	return &IAMFilerStore{client: client}
}

func (ifs *IAMFilerStore) getIAMConfigRequest() *filer_pb.LookupDirectoryEntryRequest {
	return &filer_pb.LookupDirectoryEntryRequest{
		Directory: iamConfigPrefix,
		Name:      iamIdentityFile,
	}
}

func (ifs *IAMFilerStore) LoadIAMConfig(config *iam_pb.S3ApiConfiguration) error {
	resp, err := filer_pb.LookupEntry(*ifs.client, ifs.getIAMConfigRequest())
	if err != nil {
		return err
	}
	err = ifs.loadIAMConfigFromEntryExtended(&resp.Entry.Extended, config)
	if err != nil {
		return err
	}
	return nil
}

func (ifs *IAMFilerStore) SaveIAMConfig(config *iam_pb.S3ApiConfiguration) error {
	entry := &filer_pb.Entry{
		Name:        iamIdentityFile,
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:       time.Now().Unix(),
			Crtime:      time.Now().Unix(),
			FileMode:    uint32(0644),
			Collection:  "",
			Replication: "",
		},
		Extended: make(map[string][]byte),
	}

	err := ifs.saveIAMConfigToEntryExtended(&entry.Extended, config)
	if err != nil {
		return err
	}

	_, err = filer_pb.LookupEntry(*ifs.client, ifs.getIAMConfigRequest())
	if err == filer_pb.ErrNotFound {
		err = filer_pb.CreateEntry(*ifs.client, &filer_pb.CreateEntryRequest{
			Directory:          iamConfigPrefix,
			Entry:              entry,
			IsFromOtherCluster: false,
			Signatures:         nil,
		})
	} else {
		err = filer_pb.UpdateEntry(*ifs.client, &filer_pb.UpdateEntryRequest{
			Directory:          iamConfigPrefix,
			Entry:              entry,
			IsFromOtherCluster: false,
			Signatures:         nil,
		})
	}
	if err != nil {
		return err
	}
	return nil
}

func (ifs *IAMFilerStore) loadIAMConfigFromEntryExtended(extended *map[string][]byte, config *iam_pb.S3ApiConfiguration) error {
	for _, ident := range *extended {
		identity := &iam_pb.Identity{}
		if err := proto.Unmarshal(ident, identity); err != nil {
			return err
		}
		config.Identities = append(config.Identities, identity)
	}
	return nil
}

func (ifs *IAMFilerStore) saveIAMConfigToEntryExtended(extended *map[string][]byte, config *iam_pb.S3ApiConfiguration) error {
	for _, identity := range config.Identities {
		ident, err := proto.Marshal(identity)
		if err != nil {
			return err
		}
		(*extended)[identity.Name] = ident
	}
	return nil
}
