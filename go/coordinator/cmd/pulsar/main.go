package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

func JavaStringHash(s string) uint32 {
	var h uint32
	for i, size := 0, len(s); i < size; i++ {
		h = 31*h + uint32(s[i])
	}

	return h
}

func main() {

	hash1 := JavaStringHash("key-1")
	fmt.Println(hash1)

	cfg := &pulsaradmin.Config{}
	admin, err := pulsaradmin.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	topic, _ := utils.GetTopicName("my-topic")
	admin.Topics().Delete(*topic, true, false)
	admin.Topics().Create(*topic, 1)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                           "my-topic",
		Name:                            "my-producer",
		DisableBatching:                 true,
		PartitionsAutoDiscoveryInterval: 0,
		MessageRouter:                   pulsar.NewSinglePartitionRouter(),
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		payload := "message-" + fmt.Sprint(i)
		sequenceIDValue := int64(i)
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:        "key-1",
			SequenceID: &sequenceIDValue,
			Payload:    []byte(payload),
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	producer.Flush()

	admin.Topics().Update(*topic, 2)
	result, _ := admin.Topics().GetMetadata(*topic)
	plog.Info("Get topic metadata", zap.Any("topic metadata", result))
	client.TopicPartitions("my-topic")
	time.Sleep(90 * time.Second)

	for i := 100; i <= 200; i++ {
		payload := "message-" + fmt.Sprint(i)
		sequenceIDValue := int64(i)
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:        "key-1",
			SequenceID: &sequenceIDValue,
			Payload:    []byte(payload),
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	producer.Flush()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "my-topic",
		SubscriptionName:            "my-sub",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()
	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))
	}
}
