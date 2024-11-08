package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/resolver/passthrough"
	"io"
)

func init() {
	Commands = append(Commands, &commandMountConfigure{})
}

type commandMountConfigure struct {
}

func (c *commandMountConfigure) Name() string {
	return "mount.configure"
}

func (c *commandMountConfigure) Help() string {
	return `configure the mount on current server

	mount.configure -dir=<mount_directory>

	This command connects with local mount via unix socket, so it can only run locally.
	The "mount_directory" value needs to be exactly the same as how mount was started in "weed mount -dir=<mount_directory>"

`
}

func (c *commandMountConfigure) HasTag(CommandTag) bool {
	return false
}

func (c *commandMountConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	mountConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	mountDir := mountConfigureCommand.String("dir", "", "the mount directory same as how \"weed mount -dir=<mount_directory>\" was started")
	mountQuota := mountConfigureCommand.Int("quotaMB", 0, "the quota in MB")
	if err = mountConfigureCommand.Parse(args); err != nil {
		return nil
	}

	mountDirHash := util.HashToInt32([]byte(*mountDir))
	if mountDirHash < 0 {
		mountDirHash = -mountDirHash
	}
	localSocket := fmt.Sprintf("/tmp/seaweedfs-mount-%d.sock", mountDirHash)

	clientConn, err := grpc.Dial("passthrough:///unix://"+localSocket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer clientConn.Close()

	client := mount_pb.NewSeaweedMountClient(clientConn)
	_, err = client.Configure(context.Background(), &mount_pb.ConfigureRequest{
		CollectionCapacity: int64(*mountQuota) * 1024 * 1024,
	})

	return
}
