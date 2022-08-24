package command

import (
	"fmt"
	"github.com/posener/complete"
	completeinstall "github.com/posener/complete/cmd/install"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
	"runtime"
)

func AutocompleteMain(commands []*Command) bool {
	subCommands := make(map[string]complete.Command)
	helpSubCommands := make(map[string]complete.Command)
	for _, cmd := range commands {
		flags := make(map[string]complete.Predictor)
		cmd.Flag.VisitAll(func(flag *flag.Flag) {
			flags["-"+flag.Name] = complete.PredictAnything
		})

		subCommands[cmd.Name()] = complete.Command{
			Flags: flags,
		}
		helpSubCommands[cmd.Name()] = complete.Command{}
	}
	subCommands["help"] = complete.Command{Sub: helpSubCommands}

	globalFlags := make(map[string]complete.Predictor)
	flag.VisitAll(func(flag *flag.Flag) {
		globalFlags["-"+flag.Name] = complete.PredictAnything
	})

	weedCmd := complete.Command{
		Sub:         subCommands,
		Flags:       globalFlags,
		GlobalFlags: complete.Flags{"-h": complete.PredictNothing},
	}
	cmp := complete.New("weed", weedCmd)

	return cmp.Complete()
}

func installAutoCompletion() bool {
	if runtime.GOOS == "windows" {
		fmt.Println("Windows is not supported")
		return false
	}

	err := completeinstall.Install("weed")
	if err != nil {
		fmt.Printf("install failed! %s\n", err)
		return false
	}
	fmt.Printf("autocompletion is enabled. Please restart your shell.\n")
	return true
}

func uninstallAutoCompletion() bool {
	if runtime.GOOS == "windows" {
		fmt.Println("Windows is not supported")
		return false
	}

	err := completeinstall.Uninstall("weed")
	if err != nil {
		fmt.Printf("uninstall failed! %s\n", err)
		return false
	}
	fmt.Printf("autocompletion is disabled. Please restart your shell.\n")
	return true
}

var cmdAutocomplete = &Command{
	Run:       runAutocomplete,
	UsageLine: "autocomplete",
	Short:     "install autocomplete",
	Long: `weed autocomplete is installed in the shell.

    Supported shells are bash, zsh, and fish.
    Windows is not supported.

`,
}

func runAutocomplete(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	return installAutoCompletion()
}

var cmdUnautocomplete = &Command{
	Run:       runUnautocomplete,
	UsageLine: "autocomplete.uninstall",
	Short:     "uninstall autocomplete",
	Long: `weed autocomplete is uninstalled in the shell.

    Windows is not supported.

`,
}

func runUnautocomplete(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	return uninstallAutoCompletion()
}
