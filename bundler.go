package main

import (
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

type Bundler struct {
	hashes          map[string][]*pubsub.Message
	jobs            chan job
	results         chan bool
	countThreshold  int
	delayThreshold  time.Duration
	ticker          *time.Ticker
	routines        int
	addMutex        sync.Mutex
	deleteMutex     sync.Mutex
	processingMutex sync.Mutex
	processing      bool
}

type job struct {
	hash     string
	messages []*pubsub.Message
}

func (b *Bundler) Add(hash string, msg *pubsub.Message) {
	start := time.Now()
	fmt.Println("acquiring lock..")
	b.addMutex.Lock()
	fmt.Printf("lock acquired in %v\n", time.Since(start))
	defer b.addMutex.Unlock()

	if _, exists := b.hashes[hash]; !exists {
		b.hashes[hash] = make([]*pubsub.Message, 0, 1)
	}

	b.hashes[hash] = append(b.hashes[hash], msg)

	if len(b.hashes) >= b.countThreshold {
		b.processQueue()
	}
}

func (b *Bundler) processQueue() {
	b.processingMutex.Lock()

	defer func() {
		b.ticker.Reset(b.delayThreshold)
		b.processing = false
		b.processingMutex.Unlock()
	}()

	if len(b.hashes) == 0 {
		return
	}

	start := time.Now()
	fmt.Printf("starting processing of %d hashes...\n", len(b.hashes))
	b.processing = true

	// Extract all jobs to prevent concurrent map operations
	jobs := make([]job, 0, len(b.hashes))
	for hash, msgs := range b.hashes {
		jobs = append(jobs, job{
			hash:     hash,
			messages: msgs,
		})
	}

	// Pass the jobs to the worker pool
	for _, j := range jobs {
		b.jobs <- j
	}

	// Wait until all workers are done
	for i := 0; i < len(jobs); i++ {
		<-b.results
	}

	fmt.Printf("bundle processing of %d hashes complete in %v\n", len(jobs), time.Since(start))
}

func (b *Bundler) startWorker() {
	for j := range b.jobs {
		b.processJob(j)
	}
}

func (b *Bundler) processJob(job job) {
	fmt.Printf("processing hash %s with %d messages\n", job.hash, len(job.messages))
	time.Sleep(mockOperationLatency)
	for _, msg := range job.messages {
		msg.Ack()
		acked.Add(1)
	}

	b.deleteMutex.Lock()
	delete(b.hashes, job.hash)
	b.deleteMutex.Unlock()
	b.results <- true
}

func (b *Bundler) startTicker() {
	go func() {
		for {
			select {
			case <-b.ticker.C:
				fmt.Print("delay threshold hit: ")
				if b.processing {
					fmt.Println("processing in progress, skipping")
				} else {
					fmt.Println("triggering processing of queue")
					b.addMutex.Lock()
					b.processQueue()
					b.addMutex.Unlock()
				}
			}
		}
	}()
}

func NewBundler(countThreshold int, delayThreshold time.Duration, routines int) *Bundler {
	b := &Bundler{
		hashes:         make(map[string][]*pubsub.Message),
		jobs:           make(chan job, routines),
		results:        make(chan bool, countThreshold),
		countThreshold: countThreshold,
		delayThreshold: delayThreshold,
		ticker:         time.NewTicker(delayThreshold),
		routines:       routines,
	}
	b.startTicker()

	for i := 0; i < routines; i++ {
		fmt.Printf("starting queue worker #%d\n", i+1)
		go b.startWorker()
	}

	return b
}
