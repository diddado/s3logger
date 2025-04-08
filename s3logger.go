package s3logger

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3LogRotator handles writing messages for a topic to a file,
// rotating the file every hour, and (in the regular operation)
// uploading it to S3 if non-empty.
type S3LogRotator struct {
	topic         string
	logDir        string
	s3Bucket      string
	s3Path        string
	flushInterval time.Duration

	mu          sync.Mutex
	currentHour string
	file        *os.File
	writer      *bufio.Writer
	s3Client    *s3.S3
	stopCh      chan struct{}
	wg          sync.WaitGroup // for background goroutines

	// New fields:
	fileCount   int   // internal counter for the current hour file number
	maxFileSize int64 // maximum allowed file size in bytes

	closeCallback func() // the function to call after closing.  Usually a WaitGroup Done()
}

// NewS3LogRotator now accepts maxFileSize (in bytes) and determines the starting fileCount.
// It first checks local files, and if none are found, queries S3.
func NewS3LogRotator(topic, logDir, s3Bucket, s3Path string, flushInterval time.Duration, s3Client *s3.S3, maxFileSize int64, closeCallback func()) (*S3LogRotator, error) {
	fr := &S3LogRotator{
		topic:         topic,
		logDir:        logDir,
		s3Bucket:      s3Bucket,
		s3Path:        s3Path,
		flushInterval: flushInterval,
		s3Client:      s3Client,
		maxFileSize:   maxFileSize,
		stopCh:        make(chan struct{}),
		closeCallback: closeCallback,
	}
	now := time.Now()
	fr.currentHour = now.Format("2006-01-02-15")
	localCount := fr.getLocalCurrentHourFileCount(fr.currentHour)
	if localCount == 0 {
		s3Count, err := fr.getS3CurrentHourFileCount(fr.currentHour)
		if err != nil {
			log.Printf("Error checking S3 for current hour files: %v", err)
		}
		if s3Count > 0 {
			fr.fileCount = s3Count + 1
		} else {
			fr.fileCount = 1
		}
	} else {
		fr.fileCount = localCount // Not adding 1 because we want to use the existing file, even if we might go over
	}

	if err := fr.openNewFile(fr.currentHour, fr.fileCount); err != nil {
		return nil, err
	}

	fr.scanAndUploadOldFiles() // upload any previous hour files
	fr.wg.Add(2)
	go fr.flushTicker()
	go fr.rotationTicker()
	return fr, nil
}

func (fr *S3LogRotator) GetTopic() string {
	return fr.topic
}

// getLocalCurrentHourFileCount scans the local logDir for files matching the current hour pattern.
func (fr *S3LogRotator) getLocalCurrentHourFileCount(hour string) int {
	maxCount := 0
	entries, err := os.ReadDir(fr.logDir)
	if err != nil {
		log.Printf("Error reading data directory %s: %v", fr.logDir, err)
		return 0
	}
	// Expected filename: {topic}-{hour}-{counter}.log
	prefix := fmt.Sprintf("%s-%s-", fr.topic, hour)
	suffix := ".log"
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			// Extract the counter portion.
			// filename format: topic-hour-XXX.log
			counterPart := name[len(prefix) : len(name)-len(suffix)]
			var count int
			if _, err := fmt.Sscanf(counterPart, "%03d", &count); err == nil {
				if count > maxCount {
					maxCount = count
				}
			}
		}
	}
	return maxCount
}

// getS3CurrentHourFileCount queries S3 for objects matching the current hour and returns the highest counter.
func (fr *S3LogRotator) getS3CurrentHourFileCount(hour string) (int, error) {
	maxCount := 0
	// Construct the S3 prefix. Our S3 keys will be in the format:
	// {s3Path}/{topic}/{year}/{month}/{topic}-{hour}-{counter}.log.gz
	year := hour[0:4]
	month := hour[5:7]
	prefix := fmt.Sprintf("%s/%s/%s/%s/%s-%s-", fr.s3Path, fr.topic, year, month, fr.topic, hour)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(fr.s3Bucket),
		Prefix: aws.String(prefix),
	}

	err := fr.s3Client.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			// obj.Key example: "prefix/topic/2025/03/topic-2025-03-08-04-001.log.gz"
			// Extract counter from the key.
			keyParts := strings.Split(*obj.Key, "/")
			if len(keyParts) < 5 {
				continue
			}
			filename := keyParts[len(keyParts)-1] // e.g. topic-2025-03-08-04-001.log.gz
			// Remove extension.
			filename = strings.TrimSuffix(filename, ".log.gz")

			// Expected filename: {topic}-{hour}-{counter}
			parts := strings.Split(filename, "-")
			if len(parts) < 6 {
				continue
			}
			counterStr := parts[len(parts)-1]
			var count int
			if _, err := fmt.Sscanf(counterStr, "%03d", &count); err == nil {
				if count > maxCount {
					maxCount = count
				}
			}
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	return maxCount, nil
}

// openNewFile creates (or appends to) a file for the given hour and counter.
func (fr *S3LogRotator) openNewFile(hour string, count int) error {
	filename := fmt.Sprintf("%s-%s-%03d.log", fr.topic, hour, count)
	fullpath := filepath.Join(fr.logDir, filename)
	file, err := os.OpenFile(fullpath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fullpath, err)
	}
	fr.file = file
	fr.writer = bufio.NewWriter(file)
	log.Printf("Opened new file: %s", fullpath)
	return nil
}

// rotateHourly rotates the file because a new hour has begun.
func (fr *S3LogRotator) rotateHourly() error {
	if err := fr.closeAndUploadCurrentFile(); err != nil {
		log.Printf("Error during hourly rotation for topic %s: %v", fr.topic, err)
	}
	now := time.Now()
	fr.currentHour = now.Format("2006-01-02-15")
	// Reset file counter based on local files.
	localCount := fr.getLocalCurrentHourFileCount(fr.currentHour)
	if localCount == 0 {
		s3Count, err := fr.getS3CurrentHourFileCount(fr.currentHour)
		if err != nil {
			log.Printf("Error checking S3 for current hour files: %v", err)
		}
		if s3Count > 0 {
			fr.fileCount = s3Count + 1
		} else {
			fr.fileCount = 1
		}
	} else {
		fr.fileCount = localCount + 1
	}
	return fr.openNewFile(fr.currentHour, fr.fileCount)
}

// rotateSize rotates the file because it exceeded the maximum size.
func (fr *S3LogRotator) rotateSize() error {
	if err := fr.closeAndUploadCurrentFile(); err != nil {
		log.Printf("Error during size-based rotation for topic %s: %v", fr.topic, err)
	}
	// For size-based rotation, the hour remains the same. Simply increment the counter.
	fr.fileCount++
	return fr.openNewFile(fr.currentHour, fr.fileCount)
}

// closeAndUploadCurrentFile flushes and closes the current file, compresses it with gzip,
// and uploads the compressed file to S3. After upload the local file is removed.
func (fr *S3LogRotator) closeAndUploadCurrentFile() error {
	if fr.file != nil {
		fr.writer.Flush()
		info, err := fr.file.Stat()
		oldFileName := fr.file.Name()
		if err != nil {
			log.Printf("Error stating file: %v", err)
		} else if info.Size() > 0 {
			// Close the current file and reopen for reading.
			fr.file.Close()
			f, err := os.Open(oldFileName)
			if err != nil {
				log.Printf("Error reopening file %s: %v", oldFileName, err)
			} else {
				if err := fr.uploadToS3(f); err != nil {
					log.Printf("Error uploading file for topic %s: %v", fr.topic, err)
				}
				f.Close()
			}
			os.Remove(oldFileName)
		} else {
			log.Printf("File %s for topic %s is empty. Removing.", oldFileName, fr.topic)
			fr.file.Close()
			os.Remove(oldFileName)
		}
		fr.file = nil
	}
	fr.scanAndUploadOldFiles() // pick up any stragglers
	return nil
}

// flushTicker flushes the current writer every flushInterval.
func (fr *S3LogRotator) flushTicker() {
	defer fr.wg.Done()
	ticker := time.NewTicker(fr.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fr.mu.Lock()
			if fr.writer != nil {
				fr.writer.Flush()
			}
			fr.mu.Unlock()
		case <-fr.stopCh:
			return
		}
	}
}

func (fr *S3LogRotator) rotationTicker() {
	defer fr.wg.Done()
	for {
		now := time.Now()
		nextHour := now.Truncate(time.Hour).Add(time.Hour)
		select {
		case <-time.After(time.Until(nextHour)):
			fr.mu.Lock()
			if err := fr.rotateHourly(); err != nil {
				log.Printf("Error in hourly rotation for topic %s: %v", fr.topic, err)
			}
			// Also, after hourly rotation, scan for any files from previous hours and process them.
			fr.scanAndUploadOldFiles()
			fr.mu.Unlock()
		case <-fr.stopCh:
			return
		}
	}
}

// scanAndUploadOldFiles scans the data directory for files from previous hours and processes them.
func (fr *S3LogRotator) scanAndUploadOldFiles() {
	now := time.Now()
	currentHour := now.Format("2006-01-02-15")
	entries, err := os.ReadDir(fr.logDir)
	if err != nil {
		log.Printf("Error reading log directory %s: %v", fr.logDir, err)
		return
	}
	for _, entry := range entries {
		filename := entry.Name()
		// Expect files in the format: {topic}-{hour}-{counter}.log
		if !strings.HasPrefix(filename, fr.topic+"-") || !strings.HasSuffix(filename, ".log") {
			continue
		}
		// Extract the hour portion.
		// Filename example: topic-2025-03-08-04-001.log
		// we need to have a topic name without dashes
		cleanTopicName := strings.Replace(filename, fr.topic, "topic", 1)
		parts := strings.Split(cleanTopicName, "-")
		if len(parts) < 6 {
			continue
		}
		// Reconstruct hour as: parts[1]-parts[2]-parts[3]-parts[4]
		hourPart := fmt.Sprintf("%s-%s-%s-%s", parts[1], parts[2], parts[3], parts[4])
		if hourPart < currentHour {
			fullpath := filepath.Join(fr.logDir, filename)
			f, err := os.Open(fullpath)
			if err != nil {
				log.Printf("Error opening file %s: %v", fullpath, err)
				continue
			}
			info, err := entry.Info()
			if err != nil {
				log.Printf("Error stating file %s: %v", fullpath, err)
				f.Close()
				continue
			}
			if info.Size() > 0 {
				f.Seek(0, io.SeekStart)
				if err := fr.uploadToS3(f); err != nil {
					log.Printf("Error uploading file %s for topic %s: %v", filename, fr.topic, err)
					f.Close()
					continue
				}
			} else {
				log.Printf("File %s is empty. Removing.", filename)
			}
			f.Close()
			os.Remove(fullpath)
		}
	}
}

// uploadToS3 compresses the file using gzip and uploads it to S3.
// It uses the file's name to construct the S3 key in the format:
// {s3Path}/{topic}/{year}/{month}/{filename}.gz
func (fr *S3LogRotator) uploadToS3(file *os.File) error {
	// Get the base filename.
	baseName := filepath.Base(file.Name()) // e.g. topic-2025-03-08-04-001.log
	// Parse the hour from the filename.
	cleanTopicName := strings.Replace(baseName, fr.topic, "topic", 1)
	parts := strings.Split(cleanTopicName, "-")
	if len(parts) < 6 {
		return fmt.Errorf("invalid filename format: %s", baseName)
	}
	hourPart := fmt.Sprintf("%s-%s-%s-%s", parts[1], parts[2], parts[3], parts[4])
	year := hourPart[0:4]
	month := hourPart[5:7]
	key := fmt.Sprintf("%s/%s/%s/%s/%s.log.gz", fr.s3Path, fr.topic, year, month, strings.TrimSuffix(baseName, ".log"))

	// Reset file pointer.
	file.Seek(0, io.SeekStart)
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := io.Copy(gw, file); err != nil {
		return fmt.Errorf("failed to compress file: %w", err)
	}
	if err := gw.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}
	compressedSize := int64(buf.Len())
	_, err := fr.s3Client.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(fr.s3Bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(buf.Bytes()),
		ContentLength: aws.Int64(compressedSize),
		ContentType:   aws.String("application/x-gzip"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to s3: %w", err)
	}
	log.Printf("Uploaded compressed file %s (%d bytes) to S3 bucket %s", key, compressedSize, fr.s3Bucket)
	return nil
}

// Modified Write method now also checks file size and rotates if needed.
func (fr *S3LogRotator) Write(p []byte) error {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Check if hour has changed.
	currentHour := time.Now().Format("2006-01-02-15")
	if currentHour != fr.currentHour {
		if err := fr.rotateHourly(); err != nil {
			return err
		}
	}

	if _, err := fr.writer.Write(p); err != nil {
		return err
	}
	if _, err := fr.writer.WriteString("\n"); err != nil {
		return err
	}
	if err := fr.writer.Flush(); err != nil {
		return err
	}
	info, err := fr.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() >= fr.maxFileSize {
		log.Printf("File size (%d bytes) exceeds limit (%d bytes) for topic %s, rotating file (size-based).", info.Size(), fr.maxFileSize, fr.topic)
		if err := fr.rotateSize(); err != nil {
			return err
		}
	}
	return nil
}

// Close gracefully stops the background goroutines, flushes any remaining data,
// and closes the currently open file without uploading or removing it.
func (fr *S3LogRotator) Close() error {
	defer fr.closeCallback()
	// Signal goroutines to stop.
	close(fr.stopCh)
	// Wait for background goroutines to finish.
	fr.wg.Wait()

	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Flush any remaining buffered data.
	if fr.writer != nil {
		fr.writer.Flush()
	}

	// Close the file without uploading or removing it.
	if fr.file != nil {
		if err := fr.file.Close(); err != nil {
			return err
		}
		fr.file = nil
	}
	return nil
}
