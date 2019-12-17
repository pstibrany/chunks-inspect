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
	blocks := flag.Bool("b", false, "print block details")
	lines := flag.Bool("l", false, "print log lines")
	flag.Parse()

	for _, f := range flag.Args() {
		printFile(f, *blocks, *lines)
	}
}

func printFile(filename string, blockDetails, printLines bool) {
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

	fmt.Println()
	fmt.Println("Chunks file:", filename)
	fmt.Println("UserID:", h.UserID)
	from, through := h.From.Time().In(timezone), h.Through.Time().In(timezone)
	fmt.Println("From:", from.Format(format))
	fmt.Println("Through:", through.Format(format), "("+through.Sub(from).String()+")")
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
	if blockDetails {
		fmt.Println("Found", len(lokiChunk.blocks), "block(s)")
		fmt.Println()
	} else {
		fmt.Println("Found", len(lokiChunk.blocks), "block(s), use -b to show block details")
	}

	totalSize := 0

	for ix, b := range lokiChunk.blocks {
		if blockDetails {
			fmt.Printf("Block %4d: offset: %8x, original length: %6d (stored: %6d, ratio: %0.3g), minT: %v maxT: %v\n",
				ix, b.dataOffset, b.uncompressedLength, b.dataLength, float64(b.uncompressedLength)/float64(b.dataLength),
				time.Unix(0, b.minT).In(timezone).Format(format), time.Unix(0, b.maxT).In(timezone).Format(format))
		}

		totalSize += b.uncompressedLength

		if printLines {
			for _, l := range b.entries {
				fmt.Printf("%v\t%s\n", time.Unix(0, l.timestamp).In(timezone).Format(format), strings.TrimSpace(l.line))
			}
		}
	}

	fmt.Println("Total size of uncompressed data:", totalSize, "file size:", si.Size(), "ratio:", fmt.Sprintf("%0.3g", float64(totalSize)/float64(si.Size())))
}
