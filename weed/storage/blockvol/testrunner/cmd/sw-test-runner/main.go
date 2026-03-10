package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/actions"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "run":
		runCmd(os.Args[2:])
	case "coordinator":
		coordinatorCmd(os.Args[2:])
	case "agent":
		agentCmd(os.Args[2:])
	case "console":
		consoleCmd(os.Args[2:])
	case "validate":
		validateCmd(os.Args[2:])
	case "list":
		listCmd()
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `sw-test-runner — YAML-driven test platform for SeaweedFS BlockVol

Usage:
  sw-test-runner run [flags] <scenario.yaml>           Run a test scenario (SSH mode)
  sw-test-runner coordinator [flags] <scenario.yaml>   Run as coordinator (multi-node)
  sw-test-runner agent [flags]                         Run as agent on test node
  sw-test-runner console [flags]                       Start web console server
  sw-test-runner validate <scenario.yaml>              Validate YAML without running
  sw-test-runner list [flags]                          List registered actions
  sw-test-runner help                                  Show this help

Common flags:
  -tiers <tiers>       Comma-separated enabled tiers: core,block,devops,chaos (default: all)

Run flags:
  -output <path>       Write JSON results to file
  -junit <path>        Write JUnit XML to file
  -html <path>         Write HTML report to file
  -baseline <path>     Compare against baseline JSON
  -artifacts <path>    Collect artifacts on failure to this directory

Coordinator flags:
  -port <port>         Listen port for agent registration (default: 9000)
  -token <token>       Auth token for agent communication
  -dry-run             Print execution plan without running
  -output <path>       Write JSON results to file
  -junit <path>        Write JUnit XML to file
  -html <path>         Write HTML report to file
  -artifacts <path>    Download artifacts from agents to this directory
  -timeout <duration>  Agent registration timeout (default: 30s)

Agent flags:
  -port <port>         Listen port (default: 9100)
  -coordinator <url>   Coordinator URL (e.g. http://192.168.1.100:9000)
  -token <token>       Auth token for coordinator communication
  -nodes <names>       Comma-separated node names this agent handles
  -allow-exec          Enable /exec endpoint for ad-hoc commands
  -persistent          Stay running, re-register with coordinator on each run

Console flags:
  -port <port>            Listen port (default: 9090)
  -token <token>          Auth token for agents
  -scenarios-dir <path>   Directory containing scenario YAML files
`)
}

func runCmd(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	outputPath := fs.String("output", "", "Write JSON results to file")
	junitPath := fs.String("junit", "", "Write JUnit XML to file")
	htmlPath := fs.String("html", "", "Write HTML report to file")
	baselinePath := fs.String("baseline", "", "Compare against baseline JSON")
	artifactsDir := fs.String("artifacts", "", "Collect artifacts on failure to this directory")
	tiers := fs.String("tiers", "", "Comma-separated list of enabled tiers (core,block,devops,chaos)")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: scenario file required")
		os.Exit(1)
	}
	scenarioFile := fs.Arg(0)

	logger := log.New(os.Stderr, "", log.LstdFlags)

	scenario, err := tr.ParseFile(scenarioFile)
	if err != nil {
		logger.Fatalf("parse scenario: %v", err)
	}

	// Set up signal handling.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Create registry with all actions.
	registry := tr.NewRegistry()
	actions.RegisterAll(registry)
	if *tiers != "" {
		registry.EnableTiers(parseTiers(*tiers))
	}

	logFunc := func(format string, args ...interface{}) {
		logger.Printf(format, args...)
	}

	// Create engine.
	engine := tr.NewEngine(registry, logFunc)

	// Set up infrastructure.
	actx, err := setupActionContext(scenario, logFunc)
	if err != nil {
		logger.Fatalf("setup: %v", err)
	}
	defer cleanupNodes(actx)

	// Run scenario.
	result := engine.Run(ctx, scenario, actx)

	// Print summary.
	tr.PrintSummary(os.Stdout, result)

	// Write outputs.
	if *outputPath != "" {
		if err := tr.WriteJSON(result, *outputPath); err != nil {
			logger.Printf("write JSON: %v", err)
		} else {
			logger.Printf("JSON results written to %s", *outputPath)
		}
	}

	if *junitPath != "" {
		if err := tr.WriteJUnitXML(result, *junitPath); err != nil {
			logger.Printf("write JUnit: %v", err)
		} else {
			logger.Printf("JUnit XML written to %s", *junitPath)
		}
	}

	if *htmlPath != "" {
		if err := tr.WriteHTMLReport(result, *htmlPath); err != nil {
			logger.Printf("write HTML: %v", err)
		} else {
			logger.Printf("HTML report written to %s", *htmlPath)
		}
	}

	if *baselinePath != "" {
		regressions, err := tr.BaselineCompare(result, *baselinePath)
		if err != nil {
			logger.Printf("baseline compare: %v", err)
		} else if len(regressions) > 0 {
			fmt.Fprintln(os.Stdout, "\nREGRESSIONS:")
			for _, r := range regressions {
				fmt.Fprintf(os.Stdout, "  - %s\n", r)
			}
		} else {
			fmt.Fprintln(os.Stdout, "\nNo regressions detected.")
		}
	}

	// Collect artifacts on failure.
	if result.Status == tr.StatusFail && *artifactsDir != "" {
		collectArtifacts(actx, *artifactsDir, logger)
	}

	if result.Status == tr.StatusFail {
		os.Exit(1)
	}
}

func collectArtifacts(actx *tr.ActionContext, dir string, logger *log.Logger) {
	logger.Printf("collecting artifacts to %s ...", dir)
	// Find any node for dmesg/lsblk collection.
	var clientNode *infra.Node
	for _, n := range actx.Nodes {
		if nn, ok := n.(*infra.Node); ok {
			clientNode = nn
			break
		}
	}
	if clientNode == nil {
		logger.Printf("no nodes available for artifact collection")
		return
	}

	collector := infra.NewArtifactCollector(dir, clientNode, logger)
	for name, tgt := range actx.Targets {
		if lc, ok := tgt.(infra.LogCollector); ok {
			collector.CollectLabeled(lc, name)
		}
	}
}

func coordinatorCmd(args []string) {
	fs := flag.NewFlagSet("coordinator", flag.ExitOnError)
	port := fs.Int("port", 9000, "Listen port for agent registration")
	token := fs.String("token", "", "Auth token for agent communication")
	dryRun := fs.Bool("dry-run", false, "Print execution plan without running")
	outputPath := fs.String("output", "", "Write JSON results to file")
	junitPath := fs.String("junit", "", "Write JUnit XML to file")
	htmlPath := fs.String("html", "", "Write HTML report to file")
	artifactsDir := fs.String("artifacts", "", "Download artifacts from agents to this directory")
	regTimeout := fs.String("timeout", "30s", "Agent registration timeout")
	coordTiers := fs.String("tiers", "", "Comma-separated list of enabled tiers (core,block,devops,chaos)")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: scenario file required")
		os.Exit(1)
	}
	scenarioFile := fs.Arg(0)

	logger := log.New(os.Stderr, "", log.LstdFlags)

	scenario, err := tr.ParseFile(scenarioFile)
	if err != nil {
		logger.Fatalf("parse scenario: %v", err)
	}

	// Verify scenario has agents section.
	if len(scenario.Topology.Agents) == 0 {
		logger.Fatalf("scenario has no topology.agents section; use 'run' for SSH mode")
	}

	// Set up signal handling.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Create registry.
	registry := tr.NewRegistry()
	actions.RegisterAll(registry)
	if *coordTiers != "" {
		registry.EnableTiers(parseTiers(*coordTiers))
	}

	// Create coordinator.
	coord := tr.NewCoordinator(tr.CoordinatorConfig{
		Port:     *port,
		Token:    *token,
		DryRun:   *dryRun,
		Expected: scenario.Topology.Agents,
		Logger:   log.New(os.Stderr, "[coord] ", log.LstdFlags),
	})

	if err := coord.Start(); err != nil {
		logger.Fatalf("start coordinator: %v", err)
	}
	defer coord.Stop()

	// Wait for agents.
	timeout, _ := time.ParseDuration(*regTimeout)
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	logger.Printf("waiting for %d agents (timeout=%s)...", len(scenario.Topology.Agents), timeout)
	if err := coord.WaitForAgents(ctx, timeout); err != nil {
		logger.Fatalf("%v", err)
	}
	logger.Printf("all %d agents registered", len(scenario.Topology.Agents))

	// Run scenario.
	result := coord.RunScenario(ctx, scenario, registry)

	// Download artifacts from agents (on both pass and fail).
	remoteArtifactsDir := scenario.Artifacts.Dir
	if *artifactsDir != "" && remoteArtifactsDir != "" {
		coord.DownloadAllArtifacts(ctx, remoteArtifactsDir, *artifactsDir, result)
	}

	// Print summary.
	tr.PrintSummary(os.Stdout, result)

	if *outputPath != "" {
		if err := tr.WriteJSON(result, *outputPath); err != nil {
			logger.Printf("write JSON: %v", err)
		}
	}
	if *junitPath != "" {
		if err := tr.WriteJUnitXML(result, *junitPath); err != nil {
			logger.Printf("write JUnit: %v", err)
		}
	}
	if *htmlPath != "" {
		if err := tr.WriteHTMLReport(result, *htmlPath); err != nil {
			logger.Printf("write HTML: %v", err)
		} else {
			logger.Printf("HTML report written to %s", *htmlPath)
		}
	}

	if result.Status == tr.StatusFail {
		os.Exit(1)
	}
}

func agentCmd(args []string) {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	port := fs.Int("port", 9100, "Listen port")
	coordURL := fs.String("coordinator", "", "Coordinator URL (e.g. http://192.168.1.100:9000)")
	token := fs.String("token", "", "Auth token")
	nodes := fs.String("nodes", "", "Comma-separated node names this agent handles")
	allowExec := fs.Bool("allow-exec", false, "Enable /exec endpoint")
	persistent := fs.Bool("persistent", false, "Stay running, re-register with coordinator on each run")
	fs.Parse(args)

	logger := log.New(os.Stderr, "[agent] ", log.LstdFlags)

	// Parse node names.
	var nodeNames []string
	if *nodes != "" {
		for _, n := range strings.Split(*nodes, ",") {
			n = strings.TrimSpace(n)
			if n != "" {
				nodeNames = append(nodeNames, n)
			}
		}
	}

	// Create registry.
	registry := tr.NewRegistry()
	actions.RegisterAll(registry)

	// Create agent.
	agent := tr.NewAgent(tr.AgentConfig{
		Port:           *port,
		CoordinatorURL: *coordURL,
		Token:          *token,
		AllowExec:      *allowExec,
		Persistent:     *persistent,
		Nodes:          nodeNames,
		Registry:       registry,
		Logger:         logger,
	})

	// Set up signal handling.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	logger.Printf("starting agent (nodes=%v, exec=%v, persistent=%v)", nodeNames, *allowExec, *persistent)
	if err := agent.Start(ctx); err != nil {
		logger.Fatalf("agent error: %v", err)
	}
}

func consoleCmd(args []string) {
	fs := flag.NewFlagSet("console", flag.ExitOnError)
	port := fs.Int("port", 9090, "Listen port for web UI")
	token := fs.String("token", "", "Auth token for agents")
	scenariosDir := fs.String("scenarios-dir", ".", "Directory containing scenario YAML files")
	consoleTiers := fs.String("tiers", "", "Comma-separated list of enabled tiers")
	fs.Parse(args)

	logger := log.New(os.Stderr, "[console] ", log.LstdFlags)

	registry := tr.NewRegistry()
	actions.RegisterAll(registry)
	if *consoleTiers != "" {
		registry.EnableTiers(parseTiers(*consoleTiers))
	}

	console := tr.NewConsole(tr.ConsoleConfig{
		Port:        *port,
		Token:       *token,
		ScenarioDir: *scenariosDir,
		Registry:    registry,
		Logger:      logger,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := console.Start(ctx); err != nil {
		logger.Fatalf("console error: %v", err)
	}
}

func validateCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "error: scenario file required")
		os.Exit(1)
	}

	scenario, err := tr.ParseFile(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "INVALID: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("VALID: %s (%d phases, %d targets)\n",
		scenario.Name, len(scenario.Phases), len(scenario.Targets))
}

func listCmd() {
	// Parse --tiers flag from remaining args.
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	listTiers := fs.String("tiers", "", "Comma-separated list of enabled tiers")
	fs.Parse(os.Args[2:])

	registry := tr.NewRegistry()
	actions.RegisterAll(registry)
	if *listTiers != "" {
		registry.EnableTiers(parseTiers(*listTiers))
	}

	byTier := registry.ListByTier()
	tierOrder := []string{tr.TierCore, tr.TierBlock, tr.TierDevOps, tr.TierChaos, actions.TierK8s}

	fmt.Println("Registered actions:")
	for _, tier := range tierOrder {
		names := byTier[tier]
		if len(names) == 0 {
			continue
		}
		// Skip tiers that are not enabled (if filtering).
		if len(registry.EnabledTiers) > 0 && !registry.EnabledTiers[tier] {
			continue
		}
		sort.Strings(names)
		fmt.Printf("\n  [%s]\n", tier)
		for _, name := range names {
			fmt.Printf("    - %s\n", name)
		}
	}
	fmt.Println()
}

// setupActionContext creates nodes, targets, and the action context from the scenario.
func setupActionContext(s *tr.Scenario, logFunc func(string, ...interface{})) (*tr.ActionContext, error) {
	actx := &tr.ActionContext{
		Scenario: s,
		Nodes:    make(map[string]tr.NodeRunner),
		Targets:  make(map[string]tr.TargetRunner),
		Vars:     make(map[string]string),
		Log:      logFunc,
	}

	// Create and connect nodes.
	for name, spec := range s.Topology.Nodes {
		node := &infra.Node{
			Host:    spec.Host,
			User:    spec.User,
			KeyFile: spec.KeyFile,
			IsLocal: spec.IsLocal,
		}
		if err := node.Connect(); err != nil {
			return nil, fmt.Errorf("connect node %s: %w", name, err)
		}
		actx.Nodes[name] = node
	}

	// Create targets.
	for name, spec := range s.Targets {
		nodeRunner, ok := actx.Nodes[spec.Node]
		if !ok {
			return nil, fmt.Errorf("target %s: node %s not found", name, spec.Node)
		}
		node, ok := nodeRunner.(*infra.Node)
		if !ok {
			return nil, fmt.Errorf("target %s: node %s is not infra.Node", name, spec.Node)
		}
		htSpec := infra.HATargetSpec{
			VolSize:             spec.VolSize,
			WALSize:             spec.WALSize,
			IQN:                 spec.IQN(),
			ISCSIPort:           spec.ISCSIPort,
			AdminPort:           spec.AdminPort,
			ReplicaDataPort:     spec.ReplicaDataPort,
			ReplicaCtrlPort:     spec.ReplicaCtrlPort,
			RebuildPort:         spec.RebuildPort,
			TPGID:               spec.TPGID,
			NvmePort:            spec.NvmePort,
			NQN:                 spec.NQN(),
			MaxConcurrentWrites: spec.MaxConcurrentWrites,
			NvmeIOQueues:        spec.NvmeIOQueues,
		}
		ht := infra.NewHATargetFromSpec(node, name, htSpec)
		actx.Targets[name] = ht
	}

	return actx, nil
}

func cleanupNodes(actx *tr.ActionContext) {
	for _, n := range actx.Nodes {
		n.Close()
	}
}

// parseTiers splits a comma-separated tier string into a slice.
func parseTiers(s string) []string {
	var tiers []string
	for _, t := range strings.Split(s, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			tiers = append(tiers, t)
		}
	}
	return tiers
}
