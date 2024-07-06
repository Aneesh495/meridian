package block

import (
	"bytes"
	"compress/flate"
	"io"
)

// Compress compresses input data using fast deflate compression.
func Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(src); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress decompresses deflate compressed data.
func Decompress(src []byte) ([]byte, error) {
	reader := flate.NewReader(bytes.NewReader(src))
	defer reader.Close()
	return io.ReadAll(reader)
}
