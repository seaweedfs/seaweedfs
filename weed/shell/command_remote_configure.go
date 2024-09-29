package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
	"io"
	"regexp"
	"strings"
)

func init() {
	Commands = append(Commands, &commandRemoteConfigure{})
}

type commandRemoteConfigure struct {
}

func (c *commandRemoteConfigure) Name() string {
	return "remote.configure"
}

func (c *commandRemoteConfigure) Help() string {
	return `remote storage configuration

	# see the current configurations
	remote.configure

	# set or update a configuration
	remote.configure -name=cloud1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy -s3.region=us-east-2
	remote.configure -name=cloud2 -type=gcs -gcs.appCredentialsFile=~/service-account-file.json -gcs.projectId=yyy
	remote.configure -name=cloud3 -type=azure -azure.account_name=xxx -azure.account_key=yyy
	remote.configure -name=cloud4 -type=aliyun -aliyun.access_key=xxx -aliyun.secret_key=yyy -aliyun.endpoint=oss-cn-shenzhen.aliyuncs.com -aliyun.region=cn-sehnzhen
	remote.configure -name=cloud5 -type=tencent -tencent.secret_id=xxx -tencent.secret_key=yyy -tencent.endpoint=cos.ap-guangzhou.myqcloud.com
	remote.configure -name=cloud6 -type=wasabi -wasabi.access_key=xxx -wasabi.secret_key=yyy -wasabi.endpoint=s3.us-west-1.wasabisys.com -wasabi.region=us-west-1
	remote.configure -name=cloud7 -type=storj -storj.access_key=xxx -storj.secret_key=yyy -storj.endpoint=https://gateway.us1.storjshare.io
	remote.configure -name=cloud8 -type=filebase -filebase.access_key=xxx -filebase.secret_key=yyy -filebase.endpoint=https://s3.filebase.com

	# delete one configuration
	remote.configure -delete -name=cloud1

`
}

func (c *commandRemoteConfigure) HasTag(CommandTag) bool {
	return false
}

var (
	isAlpha = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`).MatchString
)

func (c *commandRemoteConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	conf := &remote_pb.RemoteConf{}

	remoteConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	isDelete := remoteConfigureCommand.Bool("delete", false, "delete one remote storage by its name")

	remoteConfigureCommand.StringVar(&conf.Name, "name", "", "a short name to identify the remote storage")
	remoteConfigureCommand.StringVar(&conf.Type, "type", "s3", fmt.Sprintf("[%s] storage type", remote_storage.GetAllRemoteStorageNames()))

	remoteConfigureCommand.StringVar(&conf.S3AccessKey, "s3.access_key", "", "s3 access key")
	remoteConfigureCommand.StringVar(&conf.S3SecretKey, "s3.secret_key", "", "s3 secret key")
	remoteConfigureCommand.StringVar(&conf.S3Region, "s3.region", "us-east-2", "s3 region")
	remoteConfigureCommand.StringVar(&conf.S3Endpoint, "s3.endpoint", "", "endpoint for s3-compatible local object store")
	remoteConfigureCommand.StringVar(&conf.S3StorageClass, "s3.storage_class", "", "s3 storage class")
	remoteConfigureCommand.BoolVar(&conf.S3ForcePathStyle, "s3.force_path_style", true, "s3 force path style")
	remoteConfigureCommand.BoolVar(&conf.S3V4Signature, "s3.v4_signature", false, "s3 V4 signature")
	remoteConfigureCommand.BoolVar(&conf.S3SupportTagging, "s3.support_tagging", true, "s3 supportTagging")

	remoteConfigureCommand.StringVar(&conf.GcsGoogleApplicationCredentials, "gcs.appCredentialsFile", "", "google cloud storage credentials file, default to use env GOOGLE_APPLICATION_CREDENTIALS")
	remoteConfigureCommand.StringVar(&conf.GcsProjectId, "gcs.projectId", "", "google cloud storage project id, default to use env GOOGLE_CLOUD_PROJECT")

	remoteConfigureCommand.StringVar(&conf.AzureAccountName, "azure.account_name", "", "azure account name, default to use env AZURE_STORAGE_ACCOUNT")
	remoteConfigureCommand.StringVar(&conf.AzureAccountKey, "azure.account_key", "", "azure account name, default to use env AZURE_STORAGE_ACCESS_KEY")

	remoteConfigureCommand.StringVar(&conf.BackblazeKeyId, "b2.key_id", "", "backblaze keyID")
	remoteConfigureCommand.StringVar(&conf.BackblazeApplicationKey, "b2.application_key", "", "backblaze applicationKey. Note that your Master Application Key will not work with the S3 Compatible API. You must create a new key that is eligible for use. For more information: https://help.backblaze.com/hc/en-us/articles/360047425453")
	remoteConfigureCommand.StringVar(&conf.BackblazeEndpoint, "b2.endpoint", "", "backblaze endpoint")
	remoteConfigureCommand.StringVar(&conf.BackblazeRegion, "b2.region", "us-west-002", "backblaze region")

	remoteConfigureCommand.StringVar(&conf.AliyunAccessKey, "aliyun.access_key", "", "Aliyun access key, default to use env ALICLOUD_ACCESS_KEY_ID")
	remoteConfigureCommand.StringVar(&conf.AliyunSecretKey, "aliyun.secret_key", "", "Aliyun secret key, default to use env ALICLOUD_ACCESS_KEY_SECRET")
	remoteConfigureCommand.StringVar(&conf.AliyunEndpoint, "aliyun.endpoint", "", "Aliyun endpoint")
	remoteConfigureCommand.StringVar(&conf.AliyunRegion, "aliyun.region", "", "Aliyun region")

	remoteConfigureCommand.StringVar(&conf.TencentSecretId, "tencent.secret_id", "", "Tencent Secret Id, default to use env COS_SECRETID")
	remoteConfigureCommand.StringVar(&conf.TencentSecretKey, "tencent.secret_key", "", "Tencent secret key, default to use env COS_SECRETKEY")
	remoteConfigureCommand.StringVar(&conf.TencentEndpoint, "tencent.endpoint", "", "Tencent endpoint")

	remoteConfigureCommand.StringVar(&conf.BaiduAccessKey, "baidu.access_key", "", "Baidu access key, default to use env BDCLOUD_ACCESS_KEY")
	remoteConfigureCommand.StringVar(&conf.BaiduSecretKey, "baidu.secret_key", "", "Baidu secret key, default to use env BDCLOUD_SECRET_KEY")
	remoteConfigureCommand.StringVar(&conf.BaiduEndpoint, "baidu.endpoint", "", "Baidu endpoint")
	remoteConfigureCommand.StringVar(&conf.BaiduRegion, "baidu.region", "", "Baidu region")

	remoteConfigureCommand.StringVar(&conf.WasabiAccessKey, "wasabi.access_key", "", "Wasabi access key")
	remoteConfigureCommand.StringVar(&conf.WasabiSecretKey, "wasabi.secret_key", "", "Wasabi secret key")
	remoteConfigureCommand.StringVar(&conf.WasabiEndpoint, "wasabi.endpoint", "", "Wasabi endpoint, see https://wasabi.com/wp-content/themes/wasabi/docs/API_Guide/index.html#t=topics%2Fapidiff-intro.htm")
	remoteConfigureCommand.StringVar(&conf.WasabiRegion, "wasabi.region", "", "Wasabi region")

	remoteConfigureCommand.StringVar(&conf.FilebaseAccessKey, "filebase.access_key", "", "Filebase access key")
	remoteConfigureCommand.StringVar(&conf.FilebaseSecretKey, "filebase.secret_key", "", "Filebase secret key")
	remoteConfigureCommand.StringVar(&conf.FilebaseEndpoint, "filebase.endpoint", "", "Filebase endpoint, https://s3.filebase.com")

	remoteConfigureCommand.StringVar(&conf.StorjAccessKey, "storj.access_key", "", "Storj access key")
	remoteConfigureCommand.StringVar(&conf.StorjSecretKey, "storj.secret_key", "", "Storj secret key")
	remoteConfigureCommand.StringVar(&conf.StorjEndpoint, "storj.endpoint", "", "Storj endpoint")

	if err = remoteConfigureCommand.Parse(args); err != nil {
		return nil
	}

	if conf.Type != "s3" {
		// clear out the default values
		conf.S3Region = ""
		conf.S3ForcePathStyle = false
	}

	if conf.Name == "" {
		return c.listExistingRemoteStorages(commandEnv, writer)
	}

	if !isAlpha(conf.Name) {
		return fmt.Errorf("only letters and numbers allowed in name: %v", conf.Name)
	}

	if *isDelete {
		return c.deleteRemoteStorage(commandEnv, writer, conf.Name)
	}

	return c.saveRemoteStorage(commandEnv, writer, conf)

}

func (c *commandRemoteConfigure) listExistingRemoteStorages(commandEnv *CommandEnv, writer io.Writer) error {

	return filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(filer.DirectoryEtcRemote), "", func(entry *filer_pb.Entry, isLast bool) error {
		if len(entry.Content) == 0 {
			fmt.Fprintf(writer, "skipping %s\n", entry.Name)
			return nil
		}
		if !strings.HasSuffix(entry.Name, filer.REMOTE_STORAGE_CONF_SUFFIX) {
			return nil
		}
		conf := &remote_pb.RemoteConf{}

		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, entry.Name, err)
		}

		// change secret key to stars
		conf.S3SecretKey = strings.Repeat("*", len(conf.S3SecretKey))
		conf.AliyunSecretKey = strings.Repeat("*", len(conf.AliyunSecretKey))
		conf.BaiduAccessKey = strings.Repeat("*", len(conf.BaiduAccessKey))
		conf.FilebaseSecretKey = strings.Repeat("*", len(conf.FilebaseSecretKey))
		conf.StorjSecretKey = strings.Repeat("*", len(conf.StorjSecretKey))
		conf.TencentSecretKey = strings.Repeat("*", len(conf.TencentSecretKey))
		conf.WasabiSecretKey = strings.Repeat("*", len(conf.WasabiSecretKey))

		return filer.ProtoToText(writer, conf)

	})

}

func (c *commandRemoteConfigure) deleteRemoteStorage(commandEnv *CommandEnv, writer io.Writer, storageName string) error {

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:            filer.DirectoryEtcRemote,
			Name:                 storageName + filer.REMOTE_STORAGE_CONF_SUFFIX,
			IgnoreRecursiveError: false,
			IsDeleteData:         true,
			IsRecursive:          true,
			IsFromOtherCluster:   false,
			Signatures:           nil,
		}
		_, err := client.DeleteEntry(context.Background(), request)

		if err == nil {
			fmt.Fprintf(writer, "removed: %s\n", storageName)
		}

		return err

	})

}

func (c *commandRemoteConfigure) saveRemoteStorage(commandEnv *CommandEnv, writer io.Writer, conf *remote_pb.RemoteConf) error {

	data, err := proto.Marshal(conf)
	if err != nil {
		return err
	}

	if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, filer.DirectoryEtcRemote, conf.Name+filer.REMOTE_STORAGE_CONF_SUFFIX, data)
	}); err != nil && err != filer_pb.ErrNotFound {
		return err
	}

	return nil

}
