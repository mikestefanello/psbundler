package main

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
)

const (
	messagesToPublish           = 2000
	bundlerCountThreshold       = 48
	bundlerDelayThreshold       = 2 * time.Second
	bundlerGoRoutines           = 12
	topicGoroutines             = 3
	topicMaxExtension           = 10 * time.Minute
	topicMaxOutstandingMessages = 200
	mockOperationLatency        = 100 * time.Millisecond
	statusReportDelay           = 3 * time.Second
	maxHashDuplicates           = 30
)

var (
	consumed = atomic.Int32{}
	acked    = atomic.Int32{}
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	p := newPubSub()
	bundler := NewBundler(bundlerCountThreshold, bundlerDelayThreshold, bundlerGoRoutines)

	// Publish message
	p.PublishMesages()

	// Report the status
	go statusReport(cancel)

	// Consume the messages
	fmt.Println("starting message consumption")
	err := p.subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		consumed.Add(1)
		fmt.Println("message received")
		bundler.Add(string(message.Data), message)
	})

	checkError(err)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func statusReport(cancel context.CancelFunc) {
	var maxThroughout float64
	start := time.Now()
	ticker := time.NewTicker(statusReportDelay)

	for {
		select {
		case <-ticker.C:
			ackedCount := acked.Load()
			throughput := float64(ackedCount) / time.Since(start).Seconds()
			maxThroughout = math.Max(maxThroughout, throughput)

			fmt.Printf("[STATUS]: consumed %d, acked %d, acking %f/s, duration %v\n",
				consumed.Load(),
				ackedCount,
				throughput,
				time.Since(start))

			if ackedCount >= messagesToPublish {
				fmt.Println("all done.. shutting down")
				fmt.Printf("processed %d messages with max throughput: %f/s\n", ackedCount, maxThroughout)
				cancel()
			}
		}
	}
}
