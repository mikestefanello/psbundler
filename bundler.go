package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

type Bundler struct {
	config          Config
	queue           map[string]Messages
	jobs            chan job
	results         chan bool
	ticker          *time.Ticker
	addMutex        sync.Mutex
	deleteMutex     sync.Mutex
	processingMutex sync.Mutex
	processing      bool
}

type Config struct {
	CountThreshold int
	DelayThreshold time.Duration
	NumGoroutines  int
	Processor      func(key string, messages Messages)
}

func (c *Config) validate() error {
	switch {
	case c.CountThreshold < 1:
		return errors.New("must be greater than zero: CountThreshold")
	case c.DelayThreshold < 1:
		return errors.New("must be greater than zero: DelayThreshold")
	case c.NumGoroutines < 1:
		return errors.New("must be greater than zero: NumGoroutines")
	case c.Processor == nil:
		return errors.New("missing field: Processor")
	}

	return nil
}

type Messages []*pubsub.Message

func (m Messages) Ack() {
	for _, msg := range m {
		msg.Ack()
		acked.Add(1)
	}
}

func (m Messages) Nack() {
	for _, msg := range m {
		msg.Nack()
	}
}

type job struct {
	key      string
	messages Messages
}

func (b *Bundler) Add(key string, msg *pubsub.Message) {
	start := time.Now()
	fmt.Println("acquiring lock..")
	b.addMutex.Lock()
	fmt.Printf("lock acquired in %v\n", time.Since(start))
	defer b.addMutex.Unlock()

	if _, exists := b.queue[key]; !exists {
		b.queue[key] = make(Messages, 0, 1)
	}

	b.queue[key] = append(b.queue[key], msg)

	if len(b.queue) >= b.config.CountThreshold {
		b.processQueue()
	}
}

func (b *Bundler) processQueue() {
	b.processingMutex.Lock()

	defer func() {
		b.ticker.Reset(b.config.DelayThreshold)
		b.processing = false
		b.processingMutex.Unlock()
	}()

	if len(b.queue) == 0 {
		return
	}

	start := time.Now()
	fmt.Printf("starting processing of queue (size: %d)...\n", len(b.queue))
	b.processing = true

	// Extract all jobs to prevent concurrent map operations
	jobs := make([]job, 0, len(b.queue))
	for key, msgs := range b.queue {
		jobs = append(jobs, job{
			key:      key,
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

	fmt.Printf("processing of queue (size: %d) complete in %v\n", len(jobs), time.Since(start))
}

func (b *Bundler) jobWorker() {
	for j := range b.jobs {
		b.processJob(j)
	}
}

func (b *Bundler) processJob(j job) {
	fmt.Printf("processing key %s with %d messages\n", j.key, len(j.messages))
	b.config.Processor(j.key, j.messages)

	// Remove this from the queue
	b.deleteMutex.Lock()
	delete(b.queue, j.key)
	b.deleteMutex.Unlock()

	// Tell the queue processor that we're done
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

func NewBundler(cfg Config) (*Bundler, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	b := &Bundler{
		config:  cfg,
		queue:   make(map[string]Messages),
		jobs:    make(chan job, cfg.NumGoroutines),
		results: make(chan bool, cfg.CountThreshold),
		ticker:  time.NewTicker(cfg.DelayThreshold),
	}
	b.startTicker()

	for i := 0; i < cfg.NumGoroutines; i++ {
		fmt.Printf("starting queue job worker #%d\n", i+1)
		go b.jobWorker()
	}

	return b, nil
}
