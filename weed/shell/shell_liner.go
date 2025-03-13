package shell

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"io"
	"math/rand"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/peterh/liner"
)

var (
	line        *liner.State
	historyPath = path.Join(os.TempDir(), "weed-shell")
)

func RunShell(options ShellOptions) {
	slices.SortFunc(Commands, func(a, b command) int {
		return strings.Compare(a.Name(), b.Name())
	})
	line = liner.NewLiner()
	defer line.Close()
	grace.OnInterrupt(func() {
		line.Close()
	})

	line.SetCtrlCAborts(true)
	line.SetTabCompletionStyle(liner.TabPrints)

	setCompletionHandler()
	loadHistory()

	defer saveHistory()

	reg, _ := regexp.Compile(`'.*?'|".*?"|\S+`)

	commandEnv := NewCommandEnv(&options)

	ctx := context.Background()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx)
	commandEnv.MasterClient.WaitUntilConnected(ctx)

	if commandEnv.option.FilerAddress == "" {
		var filers []pb.ServerAddress
		commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
			resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
				ClientType: cluster.FilerType,
				FilerGroup: *options.FilerGroup,
			})
			if err != nil {
				return err
			}

			for _, clusterNode := range resp.ClusterNodes {
				filers = append(filers, pb.ServerAddress(clusterNode.Address))
			}
			return nil
		})
		fmt.Printf("master: %s ", *options.Masters)
		if len(filers) > 0 {
			fmt.Printf("filers: %v", filers)
			commandEnv.option.FilerAddress = filers[rand.Intn(len(filers))]
		}
		fmt.Println()
	}

	for {
		cmd, err := line.Prompt("> ")
		if err != nil {
			if err != io.EOF {
				fmt.Printf("%v\n", err)
			}
			return
		}

		for _, c := range util.StringSplit(cmd, ";") {
			if processEachCmd(reg, c, commandEnv) {
				return
			}
		}
	}
}

func processEachCmd(reg *regexp.Regexp, cmd string, commandEnv *CommandEnv) bool {
	cmds := reg.FindAllString(cmd, -1)

	line.AppendHistory(cmd)

	if len(cmds) == 0 {
		return false
	} else {

		args := make([]string, len(cmds[1:]))

		for i := range args {
			args[i] = strings.Trim(string(cmds[1+i]), "\"'")
		}

		cmd := cmds[0]
		if cmd == "help" || cmd == "?" {
			printHelp(cmds)
		} else if cmd == "exit" || cmd == "quit" {
			return true
		} else {
			foundCommand := false
			for _, c := range Commands {
				if c.Name() == cmd || c.Name() == "fs."+cmd {
					if err := c.Do(args, commandEnv, os.Stdout); err != nil {
						fmt.Fprintf(os.Stderr, "error: %v\n", err)
					}
					foundCommand = true
				}
			}
			if !foundCommand {
				fmt.Fprintf(os.Stderr, "unknown command: %v\n", cmd)
			}
		}

	}
	return false
}

func printGenericHelp() {
	msg :=
		`Type:	"help <command>" for help on <command>. Most commands support "<command> -h" also for options. 
`
	fmt.Print(msg)

	for _, c := range Commands {
		helpTexts := strings.SplitN(c.Help(), "\n", 2)
		fmt.Printf("  %-30s\t# %s \n", c.Name(), helpTexts[0])
	}
}

func printHelp(cmds []string) {
	args := cmds[1:]
	if len(args) == 0 {
		printGenericHelp()
	} else if len(args) > 1 {
		fmt.Println()
	} else {
		cmd := strings.ToLower(args[0])

		for _, c := range Commands {
			if strings.ToLower(c.Name()) == cmd {
				fmt.Printf("  %s\t# %s\n", c.Name(), c.Help())
				fmt.Printf("use \"%s -h\" for more details\n", c.Name())
			}
		}
	}
}

func setCompletionHandler() {
	line.SetCompleter(func(line string) (c []string) {
		for _, i := range Commands {
			if strings.HasPrefix(i.Name(), strings.ToLower(line)) {
				c = append(c, i.Name())
			}
		}
		return
	})
}

func loadHistory() {
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
}

func saveHistory() {
	if f, err := os.Create(historyPath); err != nil {
		fmt.Printf("Error creating history file: %v\n", err)
	} else {
		if _, err = line.WriteHistory(f); err != nil {
			fmt.Printf("Error writing history file: %v\n", err)
		}
		f.Close()
	}
}
