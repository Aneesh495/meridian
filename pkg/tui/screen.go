package tui

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// Cell represents a single character cell in the terminal grid.
type Cell struct {
	Rune  rune
	Style string
}

// Screen provides a double-buffered 2D terminal grid for flicker-free rendering.
type Screen struct {
	mu     sync.Mutex
	width  int
	height int
	front  [][]Cell
	back   [][]Cell
}

// NewScreen creates a double-buffered screen grid of width x height.
func NewScreen(width, height int) *Screen {
	if width <= 0 {
		width = 80
	}
	if height <= 0 {
		height = 24
	}

	allocBuffer := func() [][]Cell {
		b := make([][]Cell, height)
		for y := 0; y < height; y++ {
			b[y] = make([]Cell, width)
			for x := 0; x < width; x++ {
				b[y][x] = Cell{Rune: ' '}
			}
		}
		return b
	}

	return &Screen{
		width:  width,
		height: height,
		front:  allocBuffer(),
		back:   allocBuffer(),
	}
}

// Size returns screen width and height.
func (s *Screen) Size() (int, int) {
	return s.width, s.height
}

// Clear blanks the back buffer.
func (s *Screen) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for y := 0; y < s.height; y++ {
		for x := 0; x < s.width; x++ {
			s.back[y][x] = Cell{Rune: ' '}
		}
	}
}

// SetCell sets character and style at coordinate (x, y).
func (s *Screen) SetCell(x, y int, r rune, style string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if x < 0 || x >= s.width || y < 0 || y >= s.height {
		return
	}
	s.back[y][x] = Cell{Rune: r, Style: style}
}

// SetString draws text horizontally starting at (x, y).
func (s *Screen) SetString(x, y int, text string, style string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if y < 0 || y >= s.height {
		return
	}

	col := x
	for _, r := range text {
		if col >= s.width {
			break
		}
		if col >= 0 {
			s.back[y][col] = Cell{Rune: r, Style: style}
		}
		col++
	}
}

// DrawBox renders a Unicode border box with an optional title.
func (s *Screen) DrawBox(x, y, w, h int, title string, style string) {
	if w < 2 || h < 2 {
		return
	}

	// Corners
	s.SetCell(x, y, '┌', style)
	s.SetCell(x+w-1, y, '┐', style)
	s.SetCell(x, y+h-1, '└', style)
	s.SetCell(x+w-1, y+h-1, '┘', style)

	// Horizontal borders
	for i := x + 1; i < x+w-1; i++ {
		s.SetCell(i, y, '─', style)
		s.SetCell(i, y+h-1, '─', style)
	}

	// Vertical borders
	for j := y + 1; j < y+h-1; j++ {
		s.SetCell(x, j, '│', style)
		s.SetCell(x+w-1, j, '│', style)
	}

	// Optional title centered or left-aligned
	if len(title) > 0 && len(title)+4 < w {
		s.SetString(x+2, y, " "+title+" ", style)
	}
}

// Flush writes the minimal differential update from back to front buffer into writer.
func (s *Screen) Flush(w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	currentStyle := ""

	for y := 0; y < s.height; y++ {
		for x := 0; x < s.width; x++ {
			b := s.back[y][x]
			f := s.front[y][x]

			if b.Rune != f.Rune || b.Style != f.Style {
				// Move cursor to (x+1, y+1)
				buf.WriteString(MoveTo(x+1, y+1))
				if b.Style != currentStyle {
					if currentStyle != "" {
						buf.WriteString(Reset)
					}
					buf.WriteString(b.Style)
					currentStyle = b.Style
				}
				buf.WriteRune(b.Rune)
				s.front[y][x] = b
			}
		}
	}

	if currentStyle != "" {
		buf.WriteString(Reset)
	}

	_, err := fmt.Fprint(w, buf.String())
	return err
}

// RenderToString converts the back buffer to a string for testing.
func (s *Screen) RenderToString() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	for y := 0; y < s.height; y++ {
		for x := 0; x < s.width; x++ {
			buf.WriteRune(s.back[y][x].Rune)
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}
