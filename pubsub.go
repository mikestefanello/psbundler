package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PubSub struct {
	conn         *grpc.ClientConn
	client       *pubsub.Client
	server       *pstest.Server
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
}

func newPubSub() PubSub {
	var err error
	ctx := context.Background()

	p := PubSub{}
	p.server = pstest.NewServer()

	// Connect to the server without using TLS.
	p.conn, err = grpc.Dial(p.server.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	checkError(err)

	p.client, err = pubsub.NewClient(ctx, "project", option.WithGRPCConn(p.conn))
	checkError(err)

	// Create the topic
	p.topic, err = p.client.CreateTopic(ctx, "topic")
	checkError(err)

	// Create the subscription
	p.subscription, err = p.client.CreateSubscription(ctx, "sub", pubsub.SubscriptionConfig{Topic: p.topic})
	checkError(err)
	p.subscription.ReceiveSettings.MaxExtension = topicMaxExtension
	p.subscription.ReceiveSettings.NumGoroutines = topicGoroutines
	p.subscription.ReceiveSettings.MaxOutstandingMessages = topicMaxOutstandingMessages

	return p
}

func (p PubSub) PublishMesages() {
	// Add messages to the topic
	var published int
	var res *pubsub.PublishResult
	for i := 0; i < messagesToPublish; i++ {
		hash := md5.Sum([]byte(fmt.Sprint(i)))
		hashStr := hex.EncodeToString(hash[:])

		// Create a random amount of duplicates
		copies := rand.Intn(maxHashDuplicates-1) + 1
		for n := 0; n < copies; n++ {
			res = p.topic.Publish(context.Background(), &pubsub.Message{
				Data: []byte(hashStr),
			})
			published++

			if published >= messagesToPublish {
				break
			}
		}

		if published >= messagesToPublish {
			break
		}
	}
	fmt.Printf("published %d messages to the topic\n", published)

	// Ensure and wait until the last message was published
	_, err := res.Get(context.Background())
	checkError(err)
}
