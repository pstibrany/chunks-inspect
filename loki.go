package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

type Encoding struct {
	code     int
	name     string
	readerFn func(io.Reader) (io.Reader, error)
}

func (e Encoding) String() string {
	return e.name
}

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

var (
	encNone   = Encoding{code: 0, name: "none", readerFn: func(reader io.Reader) (io.Reader, error) { return reader, nil }}
	encGZIP   = Encoding{code: 1, name: "gzip", readerFn: func(reader io.Reader) (io.Reader, error) { return gzip.NewReader(reader) }}
	encDumb   = Encoding{code: 2, name: "dumb", readerFn: func(reader io.Reader) (io.Reader, error) { return reader, nil }}
	encLZ4    = Encoding{code: 3, name: "lz4", readerFn: func(reader io.Reader) (io.Reader, error) { return lz4.NewReader(reader), nil }}
	encSnappy = Encoding{code: 4, name: "snappy", readerFn: func(reader io.Reader) (io.Reader, error) { return snappy.NewReader(reader), nil }}

	Encodings = []Encoding{encNone, encGZIP, encDumb, encLZ4, encSnappy}
)

type LokiChunk struct {
	encoding Encoding

	blocks []LokiBlock

	metadataChecksum         uint32
	computedMetadataChecksum uint32
}

type LokiBlock struct {
	numEntries uint64 // number of log lines in this block
	minT       int64  // minimum timestamp, unix nanoseconds
	maxT       int64  // max timestamp, unix nanoseconds

	dataOffset uint64 // ofset in the data-part of chunks file
	dataLength uint64 // length of raw data

	rawData []byte // data as stored in chunk file, compressed

	rawDataDigest          []byte // Digest (sha256) of compressed data
	uncompressedDataDigest []byte // Digest (sha256) of uncompressed data

	// parsed rawData
	entries            []LokiEntry
	uncompressedLength int
	storedChecksum     uint32
	computedChecksum   uint32
}

type LokiEntry struct {
	timestamp int64
	line      string
}

func parseLokiChunk(chunkHeader *ChunkHeader, r io.Reader) (*LokiChunk, error) {
	// Loki chunks need to be loaded into memory, because some offsets are actually stored at the end.
	data := make([]byte, chunkHeader.DataLength)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("failed to read rawData for Loki chunk into memory: %w", err)
	}

	if num := binary.BigEndian.Uint32(data[0:4]); num != 0x012EE56A {
		return nil, fmt.Errorf("invalid magic number: %0x", num)
	}

	compression, err := getCompression(data[4], data[5])
	if err != nil {
		return nil, fmt.Errorf("failed to read compression: %w", err)
	}

	// return &LokiChunk{encoding: compression}, nil

	metasOffset := binary.BigEndian.Uint64(data[len(data)-8:])

	metadata := data[metasOffset : len(data)-(8+4)]

	metaChecksum := binary.BigEndian.Uint32(data[len(data)-12 : len(data)-8])
	computedMetaChecksum := crc32.Checksum(metadata, castagnoliTable)

	blocks, n := binary.Uvarint(metadata)
	if n <= 0 {
		return nil, fmt.Errorf("failed to read number of blocks")
	}
	metadata = metadata[n:]

	lokiChunk := &LokiChunk{
		encoding:                 compression,
		metadataChecksum:         metaChecksum,
		computedMetadataChecksum: computedMetaChecksum,
	}

	for ix := 0; ix < int(blocks); ix++ {
		block := LokiBlock{}
		block.numEntries, metadata, err = readUvarint(err, metadata)
		block.minT, metadata, err = readVarint(err, metadata)
		block.maxT, metadata, err = readVarint(err, metadata)
		block.dataOffset, metadata, err = readUvarint(err, metadata)
		block.dataLength, metadata, err = readUvarint(err, metadata)

		if err != nil {
			return nil, err
		}

		block.rawData = data[block.dataOffset : block.dataOffset+block.dataLength]
		d := sha256.Sum256(block.rawData)
		block.rawDataDigest = d[:]
		block.storedChecksum = binary.BigEndian.Uint32(data[block.dataOffset+block.dataLength : block.dataOffset+block.dataLength+4])
		block.computedChecksum = crc32.Checksum(block.rawData, castagnoliTable)
		block.uncompressedLength, block.entries, block.uncompressedDataDigest, err = parseLokiBlock(compression, block.rawData)
		lokiChunk.blocks = append(lokiChunk.blocks, block)
	}

	return lokiChunk, nil
}

func parseLokiBlock(compression Encoding, data []byte) (int, []LokiEntry, []byte, error) {
	r, err := compression.readerFn(bytes.NewReader(data))
	if err != nil {
		return 0, nil, nil, err
	}

	decompressed, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, nil, nil, err
	}

	digest := sha256.Sum256(decompressed)

	entries := []LokiEntry(nil)
	decompressedLen := len(decompressed)
	for len(decompressed) > 0 {
		var timestamp int64
		var lineLength uint64

		timestamp, decompressed, err = readVarint(err, decompressed)
		lineLength, decompressed, err = readUvarint(err, decompressed)
		if err != nil {
			return 0, nil, digest[:], err
		}

		if len(decompressed) < int(lineLength) {
			return 0, nil, digest[:], fmt.Errorf("not enough line data, need %d, got %d", lineLength, len(decompressed))
		}

		entries = append(entries, LokiEntry{
			timestamp: timestamp,
			line:      string(decompressed[0:lineLength]),
		})

		decompressed = decompressed[lineLength:]
	}

	return decompressedLen, entries, digest[:], nil
}

func readVarint(prevErr error, buf []byte) (int64, []byte, error) {
	if prevErr != nil {
		return 0, buf, prevErr
	}

	val, n := binary.Varint(buf)
	if n <= 0 {
		return 0, nil, fmt.Errorf("varint: %d", n)
	}
	return val, buf[n:], nil
}

func readUvarint(prevErr error, buf []byte) (uint64, []byte, error) {
	if prevErr != nil {
		return 0, buf, prevErr
	}

	val, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, nil, fmt.Errorf("varint: %d", n)
	}
	return val, buf[n:], nil
}

func getCompression(format byte, code byte) (Encoding, error) {
	if format == 1 {
		return encGZIP, nil
	}

	if format == 2 {
		for _, e := range Encodings {
			if e.code == int(code) {
				return e, nil
			}
		}

		return encNone, fmt.Errorf("unknown encoding: %d", code)
	}

	return encNone, fmt.Errorf("unknown format: %d", format)
}
