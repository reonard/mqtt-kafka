package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/eclipse/paho.mqtt.golang"
	"mqtt-kafka/producer"
	"os"
)

var channel chan [2]string
var asyncProducer sarama.AsyncProducer

func messageHandler(client mqtt.Client, message mqtt.Message) {

	channel <- [2]string{message.Topic(), string(message.Payload())}
	asyncProducer.Input() <- &sarama.ProducerMessage{
		Topic: "test",
		Key:   sarama.StringEncoder("test"),
		Value: sarama.StringEncoder(message.Payload()),
	}
}

func main() {

	//mqtt.DEBUG = log.New(os.Stdout,"[DEBUG]",log.Llongfile)

	channel = make(chan [2]string)
	kafkaBroker := []string{"120.77.245.156:9092"}

	cliOpt := mqtt.NewClientOptions()

	cliOpt.AddBroker("tcp://120.77.245.156:1883").SetClientID("hjm_1538070622").SetUsername("HJM2")

	mqttCli := mqtt.NewClient(cliOpt)

	if token := mqttCli.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	asyncProducer = producer.NewAsyncProducer(kafkaBroker)

	if token := mqttCli.Subscribe("go-mqtt/sample", 0, messageHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for mqttCli.IsConnected() {

		receiveMsg := <-channel

		fmt.Printf("接收到来自MQTT主题[%s]的消息, 内容为[%s]\n", receiveMsg[0], receiveMsg[1])
	}
}
