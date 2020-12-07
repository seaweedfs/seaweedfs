package iamapi

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
	err = ifs.loadIAMConfigFromEntry(resp.Entry, config)
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
		Content: []byte{},
	}
	err := ifs.saveIAMConfigToEntry(entry, config)
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

func (ifs *IAMFilerStore) loadIAMConfigFromEntry(entry *filer_pb.Entry, config *iam_pb.S3ApiConfiguration) error {
	if err := proto.Unmarshal(entry.Content, config); err != nil {
		return err
	}
	return nil
}

func (ifs *IAMFilerStore) saveIAMConfigToEntry(entry *filer_pb.Entry, config *iam_pb.S3ApiConfiguration) (err error) {
	entry.Content, err = proto.Marshal(config)
	if err != nil {
		return err
	}
	return nil
}
