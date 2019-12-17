package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

const format = "2006-01-02 15:04:05.000000 MST"

var timezone = time.UTC

func main() {
	lines := flag.Bool("l", false, "print log lines")
	flag.Parse()

	for _, f := range flag.Args() {
		printFile(f, *lines)
	}
}

func printFile(filename string, printLines bool) {
	f, err := os.Open(filename)
	if err != nil {
		log.Printf("%s: %v", filename, err)
		return
	}
	defer f.Close()

	si, err := f.Stat()
	if err != nil {
		log.Println("failed to stat file", err)
		return
	}

	h, err := DecodeHeader(f)
	if err != nil {
		log.Printf("%s: %v", filename, err)
		return
	}

	fmt.Println("UserID:", h.UserID)
	fmt.Println("From:", h.From.Time().In(timezone).Format(format))
	fmt.Println("Through:", h.Through.Time().In(timezone).Format(format))
	fmt.Println("Labels:")

	for _, l := range h.Metric {
		fmt.Println("\t", l.Name, "=", l.Value)
	}

	lokiChunk, err := parseLokiChunk(h, f)
	if err != nil {
		log.Printf("%s: %v", filename, err)
		return
	}

	fmt.Println("Encoding:", lokiChunk.encoding)
	fmt.Println("Found", len(lokiChunk.blocks), "block(s)")
	fmt.Println()

	totalSize := 0

	for ix, b := range lokiChunk.blocks {
		fmt.Printf("Block %4d: offset: %8x, original length: %6d (stored: %6d, ratio: %0.3g), minT: %v maxT: %v\n",
			ix, b.dataOffset, b.uncompressedLength, b.dataLength, float64(b.uncompressedLength)/float64(b.dataLength),
			time.Unix(0, b.minT).In(timezone).Format(format), time.Unix(0, b.maxT).In(timezone).Format(format))

		totalSize += b.uncompressedLength

		if printLines {
			for _, l := range b.entries {
				fmt.Printf("%v\t%s\n", time.Unix(0, l.timestamp).In(timezone).Format(format), strings.TrimSpace(l.line))
			}
		}
	}

	fmt.Println("Total size of uncompressed data:", totalSize, "file size:", si.Size(), "ratio:", fmt.Sprintf("%0.3g\n", float64(totalSize)/float64(si.Size())))
}
