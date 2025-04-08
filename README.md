# S3Logger

This project is used to write logs to a local directory, then rotate and push those logs to an s3 bucket every hour.

This was built for internal use only, and I am making it public purely to make importing it into my projects easier.

If you have suggested improvements you are free to create a pull request and I will consider them.  Having said that, the goal is to do one thing really well: Give me an easy solution to pushing logs to s3 on a regular basis without being too specific to any one project.

@TODO: Be able to set the interval for logging instead of only having 1 hour intervals

### Example program

As an example.  This program will write all messages from the configured NSQ topics to an s3 bucket every hour.

```go
package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nsqio/go-nsq"
	"github.com/diddado/s3logger" // import the s3logger package
)
// Config is the JSON configuration structure.
type Config struct {
	NSQLookupdHTTPAddress string   `json:"nsqlookupd_http_address"` // e.g. "127.0.0.1:4161"
	Topics                []string `json:"topics"`                  // topics to subscribe to
	Channel               string   `json:"channel"`                 // consumer channel (should be persistent)
	LogDir               string   `json:"log_dir"`                // local directory to write log files (e.g. "/data")
	S3Bucket              string   `json:"s3_bucket"`               // S3 bucket to upload files
	S3Path                string   `json:"s3_path"`                 // S3 key prefix (e.g. "nsqlogs")
	FlushIntervalSeconds  int      `json:"flush_interval_seconds"`  // flush interval in seconds
	MaxFileSizeMB         int      `json:"max_file_size_mb"`        // flush if file exceeds this number of MB
}

// NSQHandler routes NSQ messages to the S3LogRotator.
type NSQHandler struct {
	rotator *S3LogRotator
}

// HandleMessage writes the NSQ message to the appropriate file.
func (h *NSQHandler) HandleMessage(message *nsq.Message) error {
	return h.rotator.Write(message.Body)
}

func main() {
	// Read config file path from command line.
	configPath := flag.String("config", "config.json", "Path to config file")
	flag.Parse()

	// Load configuration.
	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Error parsing config: %v", err)
	}

	// Ensure the data directory exists.
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		log.Fatalf("Error creating data directory: %v", err)
	}

	flushInterval := time.Duration(config.FlushIntervalSeconds) * time.Second
	maxFileSize := int64(config.MaxFileSizeMB) * 1024 * 1024

	// Create an AWS session and S3 client.
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		log.Fatal("AWS_REGION environment variable must be set")
	}

	awsConfig := &aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewEnvCredentials(),
	}

	s3Endpoint := os.Getenv("AWS_S3_ENDPOINT") // e.g., "https://s3.amazonaws.com" or a custom endpoint

	if s3Endpoint != "" {
		awsConfig.Endpoint = aws.String(s3Endpoint)
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		log.Fatalf("Error creating AWS session: %v", err)
	}
	s3Client := s3.New(sess)

	// Track consumers and file rotators for shutdown.
	var consumers []*nsq.Consumer
	var rotators []*S3LogRotator
  wg := sync.WaitGroup{}

	// For each topic, create a consumer and associated S3LogRotator.
	for _, topic := range config.Topics {
		consumer, err := nsq.NewConsumer(topic, config.Channel, nsq.NewConfig())
		if err != nil {
			log.Fatalf("Error creating consumer for topic %s: %v", topic, err)
		}
		rotator, err := NewS3LogRotator(topic, config.DataDir, config.S3Bucket, config.S3Path, flushInterval, s3Client, maxFileSize, wg.Done)
		if err != nil {
			log.Fatalf("Error creating file rotator for topic %s: %v", topic, err)
		}
		consumer.AddHandler(&NSQHandler{rotator: rotator})
		if err := consumer.ConnectToNSQLookupd(config.NSQLookupdHTTPAddress); err != nil {
			log.Fatalf("Error connecting to nsqlookupd for topic %s: %v", topic, err)
		}
		log.Printf("Started consumer for topic %s", topic)
		consumers = append(consumers, consumer)
		rotators = append(rotators, rotator)
    wg.Add(1)
	}

	// Set up signal handling to gracefully shut down on termination.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutdown signal received, shutting down gracefully...")

	// Stop all NSQ consumers.
	for _, consumer := range consumers {
		consumer.Stop()
	}

  // Wait for consumers to finish (optional: add proper waitgroup logic if needed)
  time.Sleep(3 * time.Second)

	// Close all file rotators.
	for _, rotator := range rotators {
		if err := rotator.Close(); err != nil {
			log.Printf("Error closing file rotator for topic %s: %v", rotator.topic, err)
		} else {
			log.Printf("Successfully closed file rotator for topic %sv", rotator.topic)
		}
	}

  wg.Wait() // Wait for all files to get written and flushed

	log.Println("Shutdown complete.")
}

```
### Configuration file

The related config.json file:

```json
{
  "nsqlookupd_http_address": "nsqlookupd:4161",
  "channel": "my-channel",
  "log_dir": "/logs",
  "s3_bucket": "my-bucket",
  "s3_path": "nsqlogs",
  "flush_interval_seconds": 10,
  "max_file_size_mb": 10,
  "topics": [
      "topic1",
      "topic2",
      "topic3"
  ]
}

