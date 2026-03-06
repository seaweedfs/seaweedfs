package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

func init() {
	Commands = append(Commands, &commandBlockCreate{})
	Commands = append(Commands, &commandBlockList{})
	Commands = append(Commands, &commandBlockStatus{})
	Commands = append(Commands, &commandBlockDelete{})
	Commands = append(Commands, &commandBlockAssign{})
}

// mastersHTTP builds a comma-separated list of "http://host:port" URLs from
// the shell environment's master addresses.
func mastersHTTP(commandEnv *CommandEnv) string {
	addrs := commandEnv.MasterClient.GetMasters(context.Background())
	parts := make([]string, len(addrs))
	for i, a := range addrs {
		s := string(a)
		if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
			s = "http://" + s
		}
		parts[i] = s
	}
	return strings.Join(parts, ",")
}

// ---- block.create ----

type commandBlockCreate struct{}

func (c *commandBlockCreate) Name() string { return "block.create" }
func (c *commandBlockCreate) Help() string {
	return `create a block volume

	block.create -name <name> -size <bytes> [-replicaPlacement <xyz>] [-disk <type>] [-durability <mode>] [-replicaFactor <n>]

	durability modes: best_effort (default), sync_all, sync_quorum
	replica factor: 1, 2 (default), or 3. sync_quorum requires 3.
`
}
func (c *commandBlockCreate) HasTag(CommandTag) bool { return false }

func (c *commandBlockCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "volume name")
	size := f.Uint64("size", 0, "volume size in bytes")
	placement := f.String("replicaPlacement", "000", "placement string: 000, 001, 010, 100")
	disk := f.String("disk", "", "disk type (e.g. ssd, hdd)")
	durability := f.String("durability", "", "durability mode: best_effort (default), sync_all, sync_quorum")
	rf := f.Int("replicaFactor", 0, "replica factor: 1, 2 (default), or 3")
	if err := f.Parse(args); err != nil {
		return nil
	}
	if *name == "" || *size == 0 {
		return fmt.Errorf("both -name and -size are required")
	}

	client := blockapi.NewClient(mastersHTTP(commandEnv))
	info, err := client.CreateVolume(context.Background(), blockapi.CreateVolumeRequest{
		Name:             *name,
		SizeBytes:        *size,
		ReplicaPlacement: *placement,
		DiskType:         *disk,
		DurabilityMode:   *durability,
		ReplicaFactor:    *rf,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "created block volume %q on %s (iqn=%s, iscsi=%s)\n",
		info.Name, info.VolumeServer, info.IQN, info.ISCSIAddr)
	if info.ReplicaServer != "" {
		fmt.Fprintf(writer, "  replica on %s (iqn=%s, iscsi=%s)\n",
			info.ReplicaServer, info.ReplicaIQN, info.ReplicaISCSIAddr)
	}
	return nil
}

// ---- block.list ----

type commandBlockList struct{}

func (c *commandBlockList) Name() string { return "block.list" }
func (c *commandBlockList) Help() string {
	return `list all block volumes

	block.list
`
}
func (c *commandBlockList) HasTag(CommandTag) bool { return false }

func (c *commandBlockList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	client := blockapi.NewClient(mastersHTTP(commandEnv))
	vols, err := client.ListVolumes(context.Background())
	if err != nil {
		return err
	}
	if len(vols) == 0 {
		fmt.Fprintln(writer, "no block volumes")
		return nil
	}
	fmt.Fprintf(writer, "%-20s %-20s %-12s %-8s %-8s %-14s %-20s\n",
		"NAME", "SERVER", "SIZE", "EPOCH", "ROLE", "DURABILITY", "STATUS")
	for _, v := range vols {
		durMode := v.DurabilityMode
		if durMode == "" {
			durMode = "best_effort"
		}
		fmt.Fprintf(writer, "%-20s %-20s %-12d %-8d %-8s %-14s %-20s\n",
			v.Name, v.VolumeServer, v.SizeBytes, v.Epoch, v.Role, durMode, v.Status)
	}
	return nil
}

// ---- block.status ----

type commandBlockStatus struct{}

func (c *commandBlockStatus) Name() string { return "block.status" }
func (c *commandBlockStatus) Help() string {
	return `show status of a block volume

	block.status <name>
`
}
func (c *commandBlockStatus) HasTag(CommandTag) bool { return false }

func (c *commandBlockStatus) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: block.status <name>")
	}
	name := args[0]

	client := blockapi.NewClient(mastersHTTP(commandEnv))
	info, err := client.LookupVolume(context.Background(), name)
	if err != nil {
		return err
	}
	durMode := info.DurabilityMode
	if durMode == "" {
		durMode = "best_effort"
	}
	fmt.Fprintf(writer, "Name:           %s\n", info.Name)
	fmt.Fprintf(writer, "VolumeServer:   %s\n", info.VolumeServer)
	fmt.Fprintf(writer, "SizeBytes:      %d\n", info.SizeBytes)
	fmt.Fprintf(writer, "Epoch:          %d\n", info.Epoch)
	fmt.Fprintf(writer, "Role:           %s\n", info.Role)
	fmt.Fprintf(writer, "Status:         %s\n", info.Status)
	fmt.Fprintf(writer, "Durability:     %s\n", durMode)
	fmt.Fprintf(writer, "ISCSIAddr:      %s\n", info.ISCSIAddr)
	fmt.Fprintf(writer, "IQN:            %s\n", info.IQN)
	if info.ReplicaServer != "" {
		fmt.Fprintf(writer, "ReplicaServer:  %s\n", info.ReplicaServer)
		fmt.Fprintf(writer, "ReplicaISCSI:   %s\n", info.ReplicaISCSIAddr)
		fmt.Fprintf(writer, "ReplicaIQN:     %s\n", info.ReplicaIQN)
		fmt.Fprintf(writer, "ReplicaData:    %s\n", info.ReplicaDataAddr)
		fmt.Fprintf(writer, "ReplicaCtrl:    %s\n", info.ReplicaCtrlAddr)
	}
	return nil
}

// ---- block.delete ----

type commandBlockDelete struct{}

func (c *commandBlockDelete) Name() string { return "block.delete" }
func (c *commandBlockDelete) Help() string {
	return `delete a block volume

	block.delete <name>
`
}
func (c *commandBlockDelete) HasTag(CommandTag) bool { return false }

func (c *commandBlockDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: block.delete <name>")
	}
	name := args[0]

	client := blockapi.NewClient(mastersHTTP(commandEnv))
	if err := client.DeleteVolume(context.Background(), name); err != nil {
		return err
	}
	fmt.Fprintf(writer, "deleted block volume %q\n", name)
	return nil
}

// ---- block.assign ----

type commandBlockAssign struct{}

func (c *commandBlockAssign) Name() string { return "block.assign" }
func (c *commandBlockAssign) Help() string {
	return `assign a role to a block volume

	block.assign -name <name> -epoch <n> -role <primary|replica> [-lease <ms>]
`
}
func (c *commandBlockAssign) HasTag(CommandTag) bool { return false }

func (c *commandBlockAssign) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "volume name")
	epoch := f.Uint64("epoch", 0, "epoch number")
	role := f.String("role", "", "role: primary or replica")
	leaseTTLMs := f.Uint64("lease", 30000, "lease TTL in milliseconds")
	if err := f.Parse(args); err != nil {
		return nil
	}
	if *name == "" || *role == "" {
		return fmt.Errorf("both -name and -role are required")
	}

	client := blockapi.NewClient(mastersHTTP(commandEnv))
	if err := client.AssignRole(context.Background(), blockapi.AssignRequest{
		Name:       *name,
		Epoch:      *epoch,
		Role:       *role,
		LeaseTTLMs: *leaseTTLMs,
	}); err != nil {
		return err
	}
	fmt.Fprintf(writer, "assignment queued: %s epoch=%d role=%s lease=%dms\n",
		*name, *epoch, *role, *leaseTTLMs)
	return nil
}
