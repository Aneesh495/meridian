package tui

import (
	"fmt"
	"io"
)

// ANSI escape code constants for terminal styling and cursor control.
const (
	Reset       = "\033[0m"
	Bold        = "\033[1m"
	Dim         = "\033[2m"
	Italic      = "\033[3m"
	Underline   = "\033[4m"
	Blink       = "\033[5m"
	Reverse     = "\033[7m"
	HideCursor  = "\033[?25l"
	ShowCursor  = "\033[?25h"
	ClearScreen = "\033[2J"
	ClearLine   = "\033[2K"
	HomeCursor  = "\033[H"
)

// Color codes.
const (
	FgBlack   = "\033[30m"
	FgRed     = "\033[31m"
	FgGreen   = "\033[32m"
	FgYellow  = "\033[33m"
	FgBlue    = "\033[34m"
	FgMagenta = "\033[35m"
	FgCyan    = "\033[36m"
	FgWhite   = "\033[37m"

	FgBrightBlack   = "\033[90m"
	FgBrightRed     = "\033[91m"
	FgBrightGreen   = "\033[92m"
	FgBrightYellow  = "\033[93m"
	FgBrightBlue    = "\033[94m"
	FgBrightMagenta = "\033[95m"
	FgBrightCyan    = "\033[96m"
	FgBrightWhite   = "\033[97m"

	BgBlack   = "\033[40m"
	BgRed     = "\033[41m"
	BgGreen   = "\033[42m"
	BgYellow  = "\033[43m"
	BgBlue    = "\033[44m"
	BgMagenta = "\033[45m"
	BgCyan    = "\033[46m"
	BgWhite   = "\033[47m"
)

// MoveTo returns an escape sequence placing cursor at 1-based (x, y) coordinates.
func MoveTo(x, y int) string {
	return fmt.Sprintf("\033[%d;%dH", y, x)
}

// Colorize wraps text with style and reset codes.
func Colorize(style, text string) string {
	return style + text + Reset
}

// WriteAt writes styled text at specific coordinates to the output writer.
func WriteAt(w io.Writer, x, y int, style, text string) {
	_, _ = fmt.Fprintf(w, "%s%s%s%s", MoveTo(x, y), style, text, Reset)
}
