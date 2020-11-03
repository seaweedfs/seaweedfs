package s3api

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	proto "github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"strings"
)

func (s3a *S3ApiServer) mkdir(parentDirectoryPath string, dirName string, fn func(entry *filer_pb.Entry)) error {

	return filer_pb.Mkdir(s3a, parentDirectoryPath, dirName, fn)

}

func (s3a *S3ApiServer) mkFile(parentDirectoryPath string, fileName string, chunks []*filer_pb.FileChunk) error {

	return filer_pb.MkFile(s3a, parentDirectoryPath, fileName, chunks)

}

func (s3a *S3ApiServer) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) (entries []*filer_pb.Entry, isLast bool, err error) {

	err = filer_pb.List(s3a, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
		entries = append(entries, entry)
		if isLastEntry {
			isLast = true
		}
		return nil
	}, startFrom, inclusive, limit)

	return

}

func (s3a *S3ApiServer) rm(parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {

	return s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		err := doDeleteEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
		if err != nil {
			return err
		}

		return nil
	})

}

func doDeleteEntry(client filer_pb.SeaweedFilerClient, parentDirectoryPath string, entryName string, isDeleteData bool, isRecursive bool) error {
	request := &filer_pb.DeleteEntryRequest{
		Directory:    parentDirectoryPath,
		Name:         entryName,
		IsDeleteData: isDeleteData,
		IsRecursive:  isRecursive,
	}

	glog.V(1).Infof("delete entry %v/%v: %v", parentDirectoryPath, entryName, request)
	if resp, err := client.DeleteEntry(context.Background(), request); err != nil {
		glog.V(0).Infof("delete entry %v: %v", request, err)
		return fmt.Errorf("delete entry %s/%s: %v", parentDirectoryPath, entryName, err)
	} else {
		if resp.Error != "" {
			return fmt.Errorf("delete entry %s/%s: %v", parentDirectoryPath, entryName, resp.Error)
		}
	}
	return nil
}

func (s3a *S3ApiServer) exists(parentDirectoryPath string, entryName string, isDirectory bool) (exists bool, err error) {

	return filer_pb.Exists(s3a, parentDirectoryPath, entryName, isDirectory)

}

func loadS3config(iam *IdentityAccessManagement, option *S3ApiServerOption) error {
	return pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		resp, err := filer_pb.LookupEntry(client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/.configs",
			Name:      "s3identities",
		})
		if err != nil {
			return err
		}
		for name, ident := range resp.Entry.Extended {
			t := &Identity{
				Name:        name,
				Credentials: nil,
				Actions:     nil,
			}
			identity := &iam_pb.Identity{}
			if err := proto.Unmarshal(ident, identity); err != nil {
				return err
			}
			for _, action := range identity.Actions {
				t.Actions = append(t.Actions, Action(action))
			}
			for _, cred := range identity.Credentials {
				t.Credentials = append(t.Credentials, &Credential{
					AccessKey: cred.AccessKey,
					SecretKey: cred.SecretKey,
				})
				glog.V(0).Infof("AccessKey %s, SecretKey: %s", cred.AccessKey, cred.SecretKey)
			}
			iam.identities = append(iam.identities, t)
		}
		return nil
	}, option.FilerGrpcAddress, option.GrpcDialOption)
}

/* testing save
func saveS3config(iam *IdentityAccessManagement, option *S3ApiServerOption) (error) {
	return pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		entry := &filer_pb.Entry{
			Name:        "s3identities",
			IsDirectory: false,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:		time.Now().Unix(),
				Crtime:      time.Now().Unix(),
				FileMode:    uint32(0644),
				Collection:  "",
				Replication: "",
			},
			Extended: make(map[string][]byte),
		}
		for _, identity := range iam.identities {
			glog.V(0).Infof("get iam identities %s", identity.Name)
			i := &iam_pb.Identity{
				Name:        identity.Name,
				Credentials: []*iam_pb.Credential{},
				Actions:    []string{},
			}
			for _, cred := range identity.Credentials {
				i.Credentials = append(i.Credentials, &iam_pb.Credential{
					AccessKey: cred.AccessKey,
					SecretKey: cred.SecretKey,
				})
			}
			for _, action := range identity.Actions {
				i.Actions = append(i.Actions, string(action))
			}
			ident, err := proto.Marshal(i)
			if err != nil {
				return err
			}
			entry.Extended[identity.Name] = ident
		}
		_, err := filer_pb.LookupEntry(client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/.configs",
			Name:      "s3identities",
		})
		if err == filer_pb.ErrNotFound {
			err = filer_pb.CreateEntry(client, &filer_pb.CreateEntryRequest{
				Directory:          "/.configs",
				Entry:              entry,
				IsFromOtherCluster: false,
				Signatures:         nil,
			})
		} else {
			err = filer_pb.UpdateEntry(client, &filer_pb.UpdateEntryRequest{
				Directory:          "/.configs",
				Entry:              entry,
				IsFromOtherCluster: false,
				Signatures:         nil,
			})
		}
		return err
	},option.FilerGrpcAddress, option.GrpcDialOption)
}
*/

func objectKey(key *string) *string {
	if strings.HasPrefix(*key, "/") {
		t := (*key)[1:]
		return &t
	}
	return key
}
