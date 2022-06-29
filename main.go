package main

import (
	"flag"
	"fmt"
	"sync"
)

var (
	version string
	brokers string
	topic   string
	key     string
	data    string
	copies  uint64
)

func init() {
	flag.StringVar(&version, "v", "2.2.0", "kafka version")
	flag.StringVar(&version, "version", "2.2.0", "kafka version")

	flag.StringVar(&brokers, "b", "", "kafka brokers. multiple brokers should be separated by commas")
	flag.StringVar(&brokers, "brokers", "", "kafka brokers. multiple brokers should be separated by commas")

	flag.StringVar(&topic, "t", "", "kafka topic")
	flag.StringVar(&topic, "topic", "", "kafka topic")

	flag.StringVar(&key, "k", "", "the key you want to produce with data, default md5 of data")
	flag.StringVar(&key, "key", "", "the key you want to produce with data, default md5 of data")

	flag.StringVar(&data, "d", "", "the data you want to produce")
	flag.StringVar(&data, "data", "", "the data you want to produce")

	flag.Uint64Var(&copies, "c", 1, "how many times to duplicate the data to produce")
	flag.Uint64Var(&copies, "copy", 1, "how many times to duplicate the data to produce")
}

func main() {
	flag.Parse()

	if brokers == "" {
		fmt.Println("[Kafka tool] no brokers specified")
		return
	}
	if topic == "" {
		fmt.Println("[Kafka tool] no topic specified")
		return
	}
	if data == "" {
		fmt.Println("[Kafka tool] no data specified")
		return
	}
	producer, ok := InitSyncProducer(version, brokers)
	if !ok {
		return
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < int(copies); i++ {
		wg.Add(1)
		go func() {
			SyncProduce(producer, topic, key, data)
			wg.Done()
		}()
	}
	wg.Wait()
}
