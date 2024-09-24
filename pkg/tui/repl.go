package tui

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/Aneesh495/meridian/pkg/server"
)

// REPL provides an interactive command shell for operating on a Meridian cluster.
type REPL struct {
	client *server.Client
	reader *bufio.Reader
	writer io.Writer
	prompt string
}

// NewREPL creates a new interactive shell session.
func NewREPL(client *server.Client, in io.Reader, out io.Writer) *REPL {
	return &REPL{
		client: client,
		reader: bufio.NewReader(in),
		writer: out,
		prompt: Colorize(Bold+FgBrightCyan, "meridian") + Colorize(FgBrightWhite, " > "),
	}
}

// Run starts the interactive read-eval-print loop.
func (r *REPL) Run() error {
	_, _ = fmt.Fprintln(r.writer, Colorize(Bold+FgBrightGreen, "Meridian Interactive Shell"))
	_, _ = fmt.Fprintln(r.writer, Colorize(Dim+FgBrightWhite, "Type 'help' for command listing or 'exit' to quit.\n"))

	for {
		_, _ = fmt.Fprint(r.writer, r.prompt)
		line, err := r.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if line == "exit" || line == "quit" {
			_, _ = fmt.Fprintln(r.writer, "Bye!")
			return nil
		}

		if err := r.executeLine(line); err != nil {
			_, _ = fmt.Fprintf(r.writer, "%s %v\n", Colorize(Bold+FgRed, "Error:"), err)
		}
	}
}

func (r *REPL) executeLine(line string) error {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil
	}

	cmd := strings.ToLower(parts[0])
	ctx := context.Background()

	switch cmd {
	case "help":
		r.printHelp()
		return nil

	case "ping":
		if err := r.client.Ping(ctx); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(r.writer, Colorize(FgGreen, "PONG (cluster reachable)"))
		return nil

	case "get":
		if len(parts) < 2 {
			return fmt.Errorf("usage: get <key>")
		}
		val, err := r.client.Get(ctx, []byte(parts[1]))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(r.writer, "%s\n", string(val))
		return nil

	case "put":
		if len(parts) < 3 {
			return fmt.Errorf("usage: put <key> <value>")
		}
		val := strings.Join(parts[2:], " ")
		if err := r.client.Put(ctx, []byte(parts[1]), []byte(val)); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(r.writer, Colorize(FgGreen, "OK"))
		return nil

	case "del", "delete":
		if len(parts) < 2 {
			return fmt.Errorf("usage: del <key>")
		}
		if err := r.client.Delete(ctx, []byte(parts[1])); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(r.writer, Colorize(FgGreen, "OK"))
		return nil

	case "scan":
		prefix := ""
		if len(parts) >= 2 {
			prefix = parts[1]
		}
		limit := uint32(20)
		if len(parts) >= 3 {
			if parsed, err := strconv.ParseUint(parts[2], 10, 32); err == nil {
				limit = uint32(parsed)
			}
		}

		entries, err := r.client.Scan(ctx, []byte(prefix), limit)
		if err != nil {
			return err
		}

		_, _ = fmt.Fprintf(r.writer, "Found %d keys:\n", len(entries))
		for _, e := range entries {
			_, _ = fmt.Fprintf(r.writer, "  %-30s -> %s\n", Colorize(FgBrightYellow, string(e.Key)), string(e.Value))
		}
		return nil

	default:
		return fmt.Errorf("unknown command '%s'. Type 'help' for available commands.", cmd)
	}
}

func (r *REPL) printHelp() {
	helpText := `Available Commands:
  ping                     Test connectivity to cluster
  get <key>                Read the latest committed value for key
  put <key> <value>        Atomically write key-value with HLC commit timestamp
  del <key>                Delete key with tombstone
  scan [prefix] [limit]    Scan keys with optional prefix and limit
  help                     Show this help message
  exit, quit               Exit the shell
`
	_, _ = fmt.Fprint(r.writer, helpText)
}
