package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/duratarskeyk/logbucket/internal/bucket"
	"github.com/duratarskeyk/logbucket/internal/params"
)

func main() {
	var logsPath, logsName, logsMaxSize, dumpIntervalStr, compressProgPath, logExtension, compressedExtension string
	var rotateCount int
	flag.StringVar(&logsPath, "path", "", "Path to logs")
	flag.StringVar(&logsName, "name", "", "Log filename excluding extension")
	flag.StringVar(&logsMaxSize, "max-size", "200m", "Max log file size before rotation, valid suffixes are b, k, m, g or none(defaults to b)")
	flag.StringVar(&dumpIntervalStr, "dump-interval", "5m", "Buckets dump interval, valid suffixes are s, m, h or none(defaults to s)")
	flag.StringVar(&compressProgPath, "compress-prog", "/usr/bin/xz", "Compress logs with this program")
	flag.StringVar(&logExtension, "log-extension", "log", "Extension of the log")
	flag.StringVar(&compressedExtension, "compressed-extension", "xz", "Extension of the compressed log")
	flag.IntVar(&rotateCount, "rotate-count", 9, "Number of rotations")
	flag.Parse()
	if logsPath == "" || logsName == "" {
		flag.Usage()
		os.Exit(1)
	}
	maxSizeInBytes, ok := params.ParseMaxSize(logsMaxSize)
	if !ok {
		log.Fatalf("Error parsing max log size: %s\n", logsMaxSize)
	}
	dumpInterval, ok := params.ParseDumpInterval(dumpIntervalStr)
	if !ok {
		log.Fatalf("Error parsing dump interval: %s\n", logsMaxSize)
	}

	lineChan := make(chan string, 128)

	bucketLogger := &bucket.Bucket{
		LogPath:             logsPath,
		LogName:             logsName,
		MaxSize:             maxSizeInBytes,
		DumpInterval:        dumpInterval,
		RotateCount:         rotateCount,
		LineChan:            lineChan,
		CompressProgPath:    compressProgPath,
		LogExtension:        logExtension,
		CompressedExtension: compressedExtension,
	}
	go bucketLogger.Start()

	// Ignore sigint and sigterm, we'll die, when stdin is closed
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for {
			<-sigs
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lineChan <- scanner.Text()
	}

	bucketLogger.Stop()
}
