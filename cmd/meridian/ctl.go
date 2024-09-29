package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Aneesh495/meridian/pkg/server"
	"github.com/Aneesh495/meridian/pkg/tui"
)

func runCtl(args []string) error {
	if len(args) == 0 {
		printCtlHelp()
		return nil
	}

	subcmd := strings.ToLower(args[0])
	subargs := args[1:]

	fs := flag.NewFlagSet("ctl", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8000", "Target Meridian cluster node address")
	_ = fs.Parse(subargs)

	client, err := server.NewClient([]string{*addr}, server.DefaultClientOptions)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch subcmd {
	case "ping":
		if err := client.Ping(ctx); err != nil {
			return err
		}
		fmt.Printf("%s Node at %s is alive and responding to RPCs.\n",
			tui.Colorize(tui.Bold+tui.FgBrightGreen, "OK:"), *addr)
		return nil

	case "get":
		remaining := fs.Args()
		if len(remaining) < 1 {
			return fmt.Errorf("usage: meridian ctl get <key> [-addr=...]")
		}
		val, err := client.Get(ctx, []byte(remaining[0]))
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", string(val))
		return nil

	case "put":
		remaining := fs.Args()
		if len(remaining) < 2 {
			return fmt.Errorf("usage: meridian ctl put <key> <value> [-addr=...]")
		}
		val := strings.Join(remaining[1:], " ")
		if err := client.Put(ctx, []byte(remaining[0]), []byte(val)); err != nil {
			return err
		}
		fmt.Printf("%s Successfully wrote key '%s'\n",
			tui.Colorize(tui.Bold+tui.FgBrightGreen, "OK:"), remaining[0])
		return nil

	case "del", "delete":
		remaining := fs.Args()
		if len(remaining) < 1 {
			return fmt.Errorf("usage: meridian ctl del <key> [-addr=...]")
		}
		if err := client.Delete(ctx, []byte(remaining[0])); err != nil {
			return err
		}
		fmt.Printf("%s Successfully deleted key '%s'\n",
			tui.Colorize(tui.Bold+tui.FgBrightGreen, "OK:"), remaining[0])
		return nil

	case "scan":
		remaining := fs.Args()
		prefix := ""
		if len(remaining) >= 1 {
			prefix = remaining[0]
		}
		entries, err := client.Scan(ctx, []byte(prefix), 100)
		if err != nil {
			return err
		}
		fmt.Printf("Scanned %d entries matching prefix '%s':\n", len(entries), prefix)
		for _, e := range entries {
			fmt.Printf("  %-30s -> %s\n",
				tui.Colorize(tui.FgBrightYellow, string(e.Key)), string(e.Value))
		}
		return nil

	default:
		return fmt.Errorf("unknown ctl subcommand: %s. Run 'meridian ctl' for help.", subcmd)
	}
}

func printCtlHelp() {
	fmt.Printf(`Meridian Control CLI (ctl)

Usage:
  meridian ctl <command> [options]

Commands:
  ping                 Check liveness of target node
  get <key>            Retrieve committed value for key
  put <key> <value>    Write key-value with HLC commit timestamp
  del <key>            Delete key
  scan [prefix]        Scan multi-version snapshot keys

Flags:
  -addr string         Target cluster node address (default "127.0.0.1:8000")
`)
}
