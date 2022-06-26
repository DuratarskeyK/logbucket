package bucket

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"sync"
	"syscall"
	"time"
)

type Bucket struct {
	LogPath             string
	LogName             string
	MaxSize             uint64
	DumpInterval        time.Duration
	RotateCount         int
	LogExtension        string
	CompressedExtension string
	CompressProgPath    string

	LineChan chan string

	stopChan chan struct{}

	buckets map[string]int

	mainLog      string
	bytesWritten uint64
	currentFile  *os.File
	buf          bytes.Buffer
	rotateSync   sync.Mutex
}

func (b *Bucket) propagate(tmpName string) {
	defer b.rotateSync.Unlock()
	suffix := b.LogExtension
	if b.CompressProgPath != "" {
		suffix = fmt.Sprintf("%s.%s", b.LogExtension, b.CompressedExtension)
	}
	lastLog := path.Join(b.LogPath, fmt.Sprintf("%s.%d.%s", b.LogName, b.RotateCount, suffix))
	err := os.Remove(lastLog)
	if err != nil && !errors.Is(err, syscall.ENOENT) {
		log.Fatalf("Error removing last log: %s", err)
	}
	for i := b.RotateCount - 1; i >= 1; i-- {
		from := path.Join(b.LogPath, fmt.Sprintf("%s.%d.%s", b.LogName, i, suffix))
		to := path.Join(b.LogPath, fmt.Sprintf("%s.%d.%s", b.LogName, i+1, suffix))
		err = os.Rename(from, to)
		if err != nil && !errors.Is(err, syscall.ENOENT) {
			log.Fatalf("Failed to rotate %s to %s: %s", from, to, err)
		}
	}
	firstLog := path.Join(b.LogPath, fmt.Sprintf("%s.1.%s", b.LogName, b.LogExtension))
	err = os.Rename(tmpName, firstLog)
	if err != nil {
		log.Fatalf("Failed to rename %s to %s: %s", tmpName, firstLog, err)
	}
	if b.CompressProgPath != "" {
		cmd := exec.Command(b.CompressProgPath, firstLog)
		err := cmd.Run()
		if err != nil {
			log.Printf("Compression program failed: %s", err)
		}
	}
}

func (b *Bucket) rotate() {
	b.rotateSync.Lock()
	b.currentFile.Close()
	tmpName := path.Join(b.LogPath, fmt.Sprintf("%s.%d.%s", b.LogName, time.Now().Unix(), b.LogExtension))
	err := os.Rename(b.mainLog, tmpName)
	if err != nil {
		log.Fatalf("Error renaming %s into %s: %s", b.mainLog, tmpName, err)
	}
	b.currentFile, err = os.OpenFile(b.mainLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Can't open new logfile for writing: %s", err)
	}
	b.bytesWritten = 0
	if b.RotateCount == 0 {
		os.Remove(tmpName)
		b.rotateSync.Unlock()
		return
	}
	go b.propagate(tmpName)
}

func (b *Bucket) dump(rotate bool) {
	b.buf.Reset()
	curTime := time.Now().UTC().Format(time.RFC3339)
	for k, v := range b.buckets {
		fmt.Fprintf(&b.buf, "{\"count\":%d,\"ts\":\"%s\",%s\n", v, curTime, k)
	}
	n, err := b.buf.WriteTo(b.currentFile)
	if err != nil {
		log.Printf("Error writing to the log file: %s", err)
	}
	b.bytesWritten += uint64(n)
	if rotate && b.bytesWritten >= b.MaxSize {
		b.rotate()
	}
	b.buckets = make(map[string]int)
}

func (b *Bucket) Start() {
	b.stopChan = make(chan struct{})
	b.buckets = make(map[string]int)
	b.mainLog = path.Join(b.LogPath, fmt.Sprintf("%s.%s", b.LogName, b.LogExtension))
	fstat, err := os.Stat(b.mainLog)
	if errors.Is(err, syscall.ENOENT) {
		b.currentFile, err = os.OpenFile(b.mainLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Error openning file %s for writing: %s", b.mainLog, err)
		}
		b.bytesWritten = 0
	} else if err == nil {
		b.currentFile, err = os.OpenFile(b.mainLog, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Error openning file %s for writing: %s", b.mainLog, err)
		}
		b.bytesWritten = uint64(fstat.Size())
		if b.bytesWritten >= b.MaxSize {
			b.rotate()
		}
	} else {
		log.Fatalf("Stat of %s failed with an error: %s", b.mainLog, err)
	}

	ticker := time.NewTicker(b.DumpInterval)
	for {
		select {
		case line := <-b.LineChan:
			// expecting json hash on the input, first symbol is {
			b.buckets[line[1:]]++
		case <-ticker.C:
			b.dump(true)
		case <-b.stopChan:
			b.dump(false)
			b.stopChan <- struct{}{}
			return
		}
	}
}

func (b *Bucket) Stop() {
	b.stopChan <- struct{}{}
	<-b.stopChan
}
