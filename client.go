package main

import (
	"crypto/md5"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

// InitSyncProducer 初始化同步生产者
func InitSyncProducer(version, brokers string) (sarama.SyncProducer, bool) {
	fmt.Println("[Kafka tool] Init producer...")

	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		fmt.Printf("[Kafka tool] Init sync producer version parse failed, version=%v, err=%#v\n", version, err)
		return nil, false
	}
	config := sarama.NewConfig()
	config.Version = kafkaVersion
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	client, err := sarama.NewSyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		fmt.Printf("[Kafka tool] Init sync producer failed, version=%v, brokers=%v, err=%#v\n", version, brokers,
			err.Error())
		return nil, false
	}
	fmt.Printf("[Kafka tool] Init sync producer success, version=%v, brokers=%v\n", version, brokers)
	return client, true
}

// SyncProduce 同步生产消息
func SyncProduce(producer sarama.SyncProducer, topic, key, data string) {
	if key == "" {
		key = fmt.Sprintf("%x", md5.Sum([]byte(data)))
	}
	pid, offset, err := producer.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(data),
		})
	if err != nil {
		fmt.Printf("[Kafka tool] sync produce failed, topics=%v, key=%v, data=%v, err=%#v\n", topic, key, data,
			err.Error())
		return
	}
	fmt.Printf("[Kafka tool] sync produce success, topics=%v, key=%v, data=%v, pid=%v, offset=%v\n", topic, key, data,
		pid, offset)
}
