package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
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
	remote.configure -name=cloud1_deep -type=s3 -s3.access_key=xxx -s3.secret_key=yyy -s3.region=us-east-2 -s3.storage_class=DEEP_ARCHIVE
	remote.configure -name=cloud2 -type=gcs -gcs.appCredentialsFile=~/service-account-file.json -gcs.projectId=yyy
	remote.configure -name=cloud3 -type=azure -azure.account_name=xxx -azure.account_key=yyy
	remote.configure -name=cloud3 -type=azure -azure.account_name=xxx -azure.client_id=zzz
	remote.configure -name=cloud4 -type=aliyun -aliyun.access_key=xxx -aliyun.secret_key=yyy -aliyun.endpoint=oss-cn-shenzhen.aliyuncs.com -aliyun.region=cn-sehnzhen
	remote.configure -name=cloud5 -type=tencent -tencent.secret_id=xxx -tencent.secret_key=yyy -tencent.endpoint=cos.ap-guangzhou.myqcloud.com
	remote.configure -name=cloud6 -type=wasabi -wasabi.access_key=xxx -wasabi.secret_key=yyy -wasabi.endpoint=s3.us-west-1.wasabisys.com -wasabi.region=us-west-1
	remote.configure -name=cloud7 -type=storj -storj.access_key=xxx -storj.secret_key=yyy -storj.endpoint=https://gateway.us1.storjshare.io
	remote.configure -name=cloud8 -type=filebase -filebase.access_key=xxx -filebase.secret_key=yyy -filebase.endpoint=https://s3.filebase.com

	# tune transfer concurrency (applies to s3-compatible and azure storage)
	remote.configure -name=cloud1 -upload_concurrency=4 -download_concurrency=8

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

	fs, isDelete, uploadConcurrency, downloadConcurrency := c.configureFlagSet(conf, false)
	if err = fs.Parse(args); err != nil {
		return nil
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

	// Merge with an existing configuration so a partial update preserves
	// previously stored credentials and endpoints. Only treat a confirmed
	// missing entry as a new configuration; propagate all other load errors
	// so a transient filer failure does not overwrite stored credentials.
	typeExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "type" {
			typeExplicit = true
		}
	})
	requestedType := conf.Type
	existing, loadErr := c.loadRemoteStorageConf(commandEnv, conf.Name)
	if loadErr != nil && !errors.Is(loadErr, filer_pb.ErrNotFound) {
		return fmt.Errorf("load existing configuration %s: %v", conf.Name, loadErr)
	}
	if existing != nil {
		conf = existing
		// On an explicit type transition, reset backend-specific fields to
		// the destination defaults before re-parsing so explicit flags
		// override. An omitted -type keeps the stored backend.
		if typeExplicit && requestedType != existing.Type {
			conf.Type = requestedType
			c.applyTypeDefaults(conf)
		}
		fs, isDelete, uploadConcurrency, downloadConcurrency = c.configureFlagSet(conf, true)
		if err = fs.Parse(args); err != nil {
			return nil
		}
	}

	if err = applyConcurrency(conf, *uploadConcurrency, *downloadConcurrency); err != nil {
		return err
	}

	if conf.Type != "s3" {
		conf.S3Region = ""
		conf.S3ForcePathStyle = false
	}

	return c.saveRemoteStorage(commandEnv, writer, conf)

}

// configureFlagSet builds the remote.configure flag set bound to conf. When
// existing is true, flag defaults are taken from conf so omitted flags preserve
// prior values instead of being reset to the hard-coded new-config defaults.
func (c *commandRemoteConfigure) configureFlagSet(conf *remote_pb.RemoteConf, existing bool) (fs *flag.FlagSet, isDelete *bool, uploadConcurrency *int, downloadConcurrency *int) {
	fs = flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	isDelete = fs.Bool("delete", false, "delete one remote storage by its name")

	fs.StringVar(&conf.Name, "name", conf.Name, "a short name to identify the remote storage")
	typeDefault := "s3"
	if existing {
		typeDefault = conf.Type
	}
	fs.StringVar(&conf.Type, "type", typeDefault, fmt.Sprintf("[%s] storage type", remote_storage.GetAllRemoteStorageNames()))

	uploadConcurrency = fs.Int("upload_concurrency", int(conf.UploadConcurrency), "concurrent part uploads per file (0 = client default: s3 1, azure 16)")
	downloadConcurrency = fs.Int("download_concurrency", int(conf.DownloadConcurrency), "concurrent part downloads per read (0 = client default: s3 5, azure 16)")

	fs.StringVar(&conf.S3AccessKey, "s3.access_key", conf.S3AccessKey, "s3 access key")
	fs.StringVar(&conf.S3SecretKey, "s3.secret_key", conf.S3SecretKey, "s3 secret key")
	s3RegionDefault := "us-east-2"
	if existing {
		s3RegionDefault = conf.S3Region
	}
	fs.StringVar(&conf.S3Region, "s3.region", s3RegionDefault, "s3 region")
	fs.StringVar(&conf.S3Endpoint, "s3.endpoint", conf.S3Endpoint, "endpoint for s3-compatible local object store")
	fs.StringVar(&conf.S3StorageClass, "s3.storage_class", conf.S3StorageClass, "s3 storage class")
	s3ForcePathStyleDefault := true
	if existing {
		s3ForcePathStyleDefault = conf.S3ForcePathStyle
	}
	fs.BoolVar(&conf.S3ForcePathStyle, "s3.force_path_style", s3ForcePathStyleDefault, "s3 force path style")
	fs.BoolVar(&conf.S3V4Signature, "s3.v4_signature", conf.S3V4Signature, "s3 V4 signature")
	s3SupportTaggingDefault := true
	if existing {
		s3SupportTaggingDefault = conf.S3SupportTagging
	}
	fs.BoolVar(&conf.S3SupportTagging, "s3.support_tagging", s3SupportTaggingDefault, "s3 supportTagging")

	fs.StringVar(&conf.GcsGoogleApplicationCredentials, "gcs.appCredentialsFile", conf.GcsGoogleApplicationCredentials, "google cloud storage credentials file, default to use env GOOGLE_APPLICATION_CREDENTIALS")
	fs.StringVar(&conf.GcsProjectId, "gcs.projectId", conf.GcsProjectId, "google cloud storage project id, default to use env GOOGLE_CLOUD_PROJECT")

	fs.StringVar(&conf.AzureAccountName, "azure.account_name", conf.AzureAccountName, "azure account name, default to use env AZURE_STORAGE_ACCOUNT")
	fs.StringVar(&conf.AzureAccountKey, "azure.account_key", conf.AzureAccountKey, "azure account key, default to use env AZURE_STORAGE_ACCESS_KEY. Leave empty to authenticate with Entra ID")
	fs.StringVar(&conf.AzureClientId, "azure.client_id", conf.AzureClientId, "azure user-assigned identity to authenticate, when no account key is given. Workload identity also reads env AZURE_TENANT_ID and AZURE_FEDERATED_TOKEN_FILE")
	fs.StringVar(&conf.AzureEndpoint, "azure.endpoint", conf.AzureEndpoint, "azure blob service url, for accounts outside the public cloud, e.g. https://xxx.blob.core.usgovcloudapi.net/")

	fs.StringVar(&conf.BackblazeKeyId, "b2.key_id", conf.BackblazeKeyId, "backblaze keyID")
	fs.StringVar(&conf.BackblazeApplicationKey, "b2.application_key", conf.BackblazeApplicationKey, "backblaze applicationKey. Note that your Master Application Key will not work with the S3 Compatible API. You must create a new key that is eligible for use. For more information: https://help.backblaze.com/hc/en-us/articles/360047425453")
	fs.StringVar(&conf.BackblazeEndpoint, "b2.endpoint", conf.BackblazeEndpoint, "backblaze endpoint")
	b2RegionDefault := "us-west-002"
	if existing {
		b2RegionDefault = conf.BackblazeRegion
	}
	fs.StringVar(&conf.BackblazeRegion, "b2.region", b2RegionDefault, "backblaze region")

	fs.StringVar(&conf.AliyunAccessKey, "aliyun.access_key", conf.AliyunAccessKey, "Aliyun access key, default to use env ALICLOUD_ACCESS_KEY_ID")
	fs.StringVar(&conf.AliyunSecretKey, "aliyun.secret_key", conf.AliyunSecretKey, "Aliyun secret key, default to use env ALICLOUD_ACCESS_KEY_SECRET")
	fs.StringVar(&conf.AliyunEndpoint, "aliyun.endpoint", conf.AliyunEndpoint, "Aliyun endpoint")
	fs.StringVar(&conf.AliyunRegion, "aliyun.region", conf.AliyunRegion, "Aliyun region")

	fs.StringVar(&conf.TencentSecretId, "tencent.secret_id", conf.TencentSecretId, "Tencent Secret Id, default to use env COS_SECRETID")
	fs.StringVar(&conf.TencentSecretKey, "tencent.secret_key", conf.TencentSecretKey, "Tencent secret key, default to use env COS_SECRETKEY")
	fs.StringVar(&conf.TencentEndpoint, "tencent.endpoint", conf.TencentEndpoint, "Tencent endpoint")

	fs.StringVar(&conf.BaiduAccessKey, "baidu.access_key", conf.BaiduAccessKey, "Baidu access key, default to use env BDCLOUD_ACCESS_KEY")
	fs.StringVar(&conf.BaiduSecretKey, "baidu.secret_key", conf.BaiduSecretKey, "Baidu secret key, default to use env BDCLOUD_SECRET_KEY")
	fs.StringVar(&conf.BaiduEndpoint, "baidu.endpoint", conf.BaiduEndpoint, "Baidu endpoint")
	fs.StringVar(&conf.BaiduRegion, "baidu.region", conf.BaiduRegion, "Baidu region")

	fs.StringVar(&conf.WasabiAccessKey, "wasabi.access_key", conf.WasabiAccessKey, "Wasabi access key")
	fs.StringVar(&conf.WasabiSecretKey, "wasabi.secret_key", conf.WasabiSecretKey, "Wasabi secret key")
	fs.StringVar(&conf.WasabiEndpoint, "wasabi.endpoint", conf.WasabiEndpoint, "Wasabi endpoint, see https://wasabi.com/wp-content/themes/wasabi/docs/API_Guide/index.html#t=topics%2Fapidiff-intro.htm")
	fs.StringVar(&conf.WasabiRegion, "wasabi.region", conf.WasabiRegion, "Wasabi region")

	fs.StringVar(&conf.FilebaseAccessKey, "filebase.access_key", conf.FilebaseAccessKey, "Filebase access key")
	fs.StringVar(&conf.FilebaseSecretKey, "filebase.secret_key", conf.FilebaseSecretKey, "Filebase secret key")
	fs.StringVar(&conf.FilebaseEndpoint, "filebase.endpoint", conf.FilebaseEndpoint, "Filebase endpoint, https://s3.filebase.com")

	fs.StringVar(&conf.StorjAccessKey, "storj.access_key", conf.StorjAccessKey, "Storj access key")
	fs.StringVar(&conf.StorjSecretKey, "storj.secret_key", conf.StorjSecretKey, "Storj secret key")
	fs.StringVar(&conf.StorjEndpoint, "storj.endpoint", conf.StorjEndpoint, "Storj endpoint")

	return
}

// maxRemoteConcurrency caps configured remote transfer concurrency so one
// configuration cannot request an unbounded number of network workers.
const maxRemoteConcurrency = 1024

// applyTypeDefaults resets backend-specific fields to their new-config defaults
// for conf.Type, used when the storage type changes on update.
func (c *commandRemoteConfigure) applyTypeDefaults(conf *remote_pb.RemoteConf) {
	conf.S3Region = ""
	conf.S3ForcePathStyle = false
	conf.S3SupportTagging = false
	conf.S3V4Signature = false
	conf.BackblazeRegion = ""
	if conf.Type == "s3" {
		conf.S3Region = "us-east-2"
		conf.S3ForcePathStyle = true
		conf.S3SupportTagging = true
	} else if conf.Type == "b2" {
		conf.BackblazeRegion = "us-west-002"
	}
}

func applyConcurrency(conf *remote_pb.RemoteConf, upload, download int) error {
	if upload < 0 || upload > maxRemoteConcurrency {
		return fmt.Errorf("upload_concurrency must be between 0 and %d", maxRemoteConcurrency)
	}
	if download < 0 || download > maxRemoteConcurrency {
		return fmt.Errorf("download_concurrency must be between 0 and %d", maxRemoteConcurrency)
	}
	conf.UploadConcurrency = uint32(upload)
	conf.DownloadConcurrency = uint32(download)
	return nil
}

func (c *commandRemoteConfigure) loadRemoteStorageConf(commandEnv *CommandEnv, name string) (*remote_pb.RemoteConf, error) {
	var conf *remote_pb.RemoteConf
	err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		content, readErr := filer.ReadInsideFiler(context.Background(), client, filer.DirectoryEtcRemote, name+filer.REMOTE_STORAGE_CONF_SUFFIX)
		if readErr != nil {
			return readErr
		}
		conf = &remote_pb.RemoteConf{}
		if unmarshalErr := proto.Unmarshal(content, conf); unmarshalErr != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, name, unmarshalErr)
		}
		return nil
	})
	return conf, err
}

func (c *commandRemoteConfigure) listExistingRemoteStorages(commandEnv *CommandEnv, writer io.Writer) error {

	return filer_pb.ReadDirAllEntries(context.Background(), commandEnv, util.FullPath(filer.DirectoryEtcRemote), "", func(entry *filer_pb.Entry, isLast bool) error {
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

		name := storageName + filer.REMOTE_STORAGE_CONF_SUFFIX
		err := filer_pb.DoRemove(context.Background(), client, filer.DirectoryEtcRemote, name, true, true, false, false, nil)

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
		return filer.SaveInsideFiler(context.Background(), client, filer.DirectoryEtcRemote, conf.Name+filer.REMOTE_STORAGE_CONF_SUFFIX, data)
	}); err != nil && err != filer_pb.ErrNotFound {
		return err
	}

	return nil

}
