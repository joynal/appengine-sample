package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

func main() {
	fmt.Println("script initiated ------->")

	ctx := context.Background()

	// process notification json object to struct
	client, err := pubsub.NewClient(ctx, "adept-mountain-238503")
	if err != nil {
		log.Fatal(err)
	}

	sub := client.Subscription("raw-notification")
	cctx, _ := context.WithCancel(ctx)

	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		// topic configs
		senderTopic := "notification"
		topic := client.Topic(senderTopic)
		defer topic.Stop()

		topic.PublishSettings.CountThreshold = 1000
		topic.PublishSettings.BufferedByteLimit = 2e9

		// wait group for finishing all goroutines
		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < 1000000; i++ {
			result := topic.Publish(ctx, &pubsub.Message{
				Data: msg.Data,
			})

			wg.Add(1)
			go getResult(result, ctx, &wg)
		}

		wg.Wait()

		fmt.Println("elapsed:", time.Since(start))
	})

	if err != nil {
		fmt.Println(err)
	}
}

func getResult(result *pubsub.PublishResult, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	id, err := result.Get(ctx)

	if err != nil {
		fmt.Println("err: ", err)
	}

	fmt.Printf("Sent msg ID: %v\n", id)
}
