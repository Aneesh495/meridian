package main

import (
	"fmt"
	"os"

	"github.com/Aneesh495/meridian/pkg/server"
	"github.com/Aneesh495/meridian/pkg/tui"
)

const Version = "0.9.4"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "start":
		err = runStart(args)

	case "ctl":
		err = runCtl(args)

	case "bench":
		err = runBench(args)

	case "chaos":
		err = runChaos(args)

	case "repl":
		addr := "127.0.0.1:8000"
		if len(args) > 0 {
			addr = args[0]
		}
		client, dialErr := server.NewClient([]string{addr}, server.DefaultClientOptions)
		if dialErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", addr, dialErr)
			os.Exit(1)
		}
		defer client.Close()
		repl := tui.NewREPL(client, os.Stdin, os.Stdout)
		err = repl.Run()

	case "version", "--version", "-v":
		fmt.Printf("meridian version %s (darwin/arm64, pure Go)\n", Version)
		return

	case "help", "--help", "-h":
		printUsage()
		return

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\nRun 'meridian help' for available subcommands.\n", cmd)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %v\n", tui.Colorize(tui.Bold+tui.FgRed, "Error:"), err)
		os.Exit(1)
	}
}

func printUsage() {
	banner := `
   __  ___          _     _ _           
  /  |/  /__ ______(_)___/ (_)__ ____   
 / /|_/ / -_) __/ / / _  / / _ \/ _ \  
/_/  /_/\__/_/ /_/_/\_,_/_/\_/\_/_//_/  
`
	fmt.Print(tui.Colorize(tui.Bold+tui.FgBrightCyan, banner))
	fmt.Printf("Distributed State Machine & Durable Execution Fabric v%s\n\n", Version)
	fmt.Println("Usage:")
	fmt.Println("  meridian <command> [arguments]")
	fmt.Println()
	fmt.Println("Core Commands:")
	fmt.Println("  start     Boot a Meridian Multi-Raft cluster node daemon")
	fmt.Println("  ctl       Inspect and manage cluster state and keys")
	fmt.Println("  repl      Launch interactive terminal shell")
	fmt.Println("  bench     Run high-throughput synthetic performance benchmark")
	fmt.Println("  chaos     Execute deterministic simulation testing & WGL linearizability checker")
	fmt.Println("  version   Display version information")
	fmt.Println()
	fmt.Println("Run 'meridian <command> -h' for command-specific flags and options.")
}
