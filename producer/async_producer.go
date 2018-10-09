package producer

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func NewAsyncProducer(brokerList []string) sarama.AsyncProducer {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 1000 * time.Microsecond

	producer, err := sarama.NewAsyncProducer(brokerList, config)

	if err != nil {
		log.Fatal("Failed to start kafka async producer")
	}

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write message, err:", err)
		}
	}()

	return producer
}
