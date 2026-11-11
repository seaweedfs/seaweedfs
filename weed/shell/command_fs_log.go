package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandFsLogPurge{})
}

type commandFsLogPurge struct {
}

// filerDiscoveryTimeout bounds the master round trip used to list the filers.
const filerDiscoveryTimeout = 30 * time.Second

func (c *commandFsLogPurge) Name() string {
	return "fs.log.purge"
}

func (c *commandFsLogPurge) Help() string {
	return `purge filer logs

	fs.log.purge [-v] [-daysAgo 365] [-filer=<host:port>,...]

	Each filer keeps its own metadata logs, so the purge runs on every filer
	registered with the master by default. Use -filer to restrict it to a
	subset of the cluster.
`
}

func (c *commandFsLogPurge) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsLogPurge) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	fsLogPurgeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	daysAgo := fsLogPurgeCommand.Uint("daysAgo", 365, "purge logs older than N days")
	verbose := fsLogPurgeCommand.Bool("v", false, "verbose mode")
	filerList := fsLogPurgeCommand.String("filer", "", "comma separated filer addresses to purge (default: all filers in the cluster)")

	if err = fsLogPurgeCommand.Parse(args); err != nil {
		return err
	}

	filers, err := resolveLogPurgeFilers(*filerList, commandEnv, *verbose, writer)
	if err != nil {
		return err
	}

	modificationTimeAgo := time.Now().Add(-time.Hour * 24 * time.Duration(*daysAgo)).Unix()

	var failed []string
	for _, filerAddress := range filers {
		if *verbose {
			fmt.Fprintf(writer, "purging filer %s\n", filerAddress)
		}
		if purgeErr := purgeFilerLogs(commandEnv, filerAddress, modificationTimeAgo, *verbose, writer); purgeErr != nil {
			// one unreachable filer must not stop the purge of the others
			fmt.Fprintf(writer, "purge filer %s: %v\n", filerAddress, purgeErr)
			failed = append(failed, fmt.Sprintf("%s: %v", filerAddress, purgeErr))
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("failed to purge %d of %d filers: %s", len(failed), len(filers), strings.Join(failed, "; "))
	}
	return nil
}

// resolveLogPurgeFilers returns the filers to purge: the explicit -filer list
// when given, otherwise every filer registered with the master.
func resolveLogPurgeFilers(filerList string, commandEnv *CommandEnv, verbose bool, writer io.Writer) ([]pb.ServerAddress, error) {
	if filerList != "" {
		var filers []pb.ServerAddress
		for _, address := range strings.Split(filerList, ",") {
			if address = strings.TrimSpace(address); address != "" {
				filers = append(filers, pb.ServerAddress(address))
			}
		}
		if len(filers) == 0 {
			return nil, fmt.Errorf("-filer %q lists no filer address", filerList)
		}
		return filers, nil
	}

	if commandEnv.MasterClient != nil && commandEnv.option.FilerGroup != nil {
		// bound the master round trip: a shell whose master connection dropped
		// must still fall back to the filer it is connected to instead of
		// waiting forever before purging anything.
		ctx, cancel := context.WithTimeout(context.Background(), filerDiscoveryTimeout)
		defer cancel()

		var filers []pb.ServerAddress
		listErr := commandEnv.MasterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
			resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
				ClientType: cluster.FilerType,
				FilerGroup: *commandEnv.option.FilerGroup,
			})
			if err != nil {
				return err
			}
			for _, node := range resp.ClusterNodes {
				filers = append(filers, pb.ServerAddress(node.Address))
			}
			return nil
		})
		if listErr == nil && len(filers) > 0 {
			return filers, nil
		}
		// a master that cannot be reached or lists nothing is not fatal: the
		// filer this shell talks to is still worth purging.
		if listErr != nil && verbose {
			fmt.Fprintf(writer, "cannot list filers from master: %v\n", listErr)
		}
	}

	commandEnv.filerMu.Lock()
	address := commandEnv.option.FilerAddress
	commandEnv.filerMu.Unlock()
	if address != "" {
		return []pb.ServerAddress{address}, nil
	}
	return nil, fmt.Errorf("no filer found to purge")
}

// purgeFilerLogs purges the logs of a single filer, leaving the caller's
// command environment untouched.
func purgeFilerLogs(commandEnv *CommandEnv, filerAddress pb.ServerAddress, modificationTimeAgo int64, verbose bool, writer io.Writer) error {
	options := *commandEnv.option
	options.FilerAddress = filerAddress
	filerEnv := &CommandEnv{
		env:          commandEnv.env,
		MasterClient: commandEnv.MasterClient,
		option:       &options,
		locker:       commandEnv.locker,
		noLock:       commandEnv.noLock,
		forceNoLock:  commandEnv.forceNoLock,
		verbose:      commandEnv.verbose,
	}

	return filer_pb.ReadDirAllEntries(context.Background(), filerEnv, filer.SystemLogDir, "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.Attributes.Mtime > modificationTimeAgo {
			return nil
		}
		if errDel := filer_pb.Remove(context.Background(), filerEnv, filer.SystemLogDir, entry.Name, true, true, true, false, nil); errDel != nil {
			return errDel
		}
		if verbose {
			fmt.Fprintf(writer, "delete %s\n", entry.Name)
		}
		return nil
	})
}
