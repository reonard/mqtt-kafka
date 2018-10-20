package main

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"mqtt-kafka/message"
	"mqtt-kafka/producer"
	"os"
)

func main() {

	//mqtt.DEBUG = log.New(os.Stdout,"[DEBUG]",log.Llongfile)

	message.Channel = make(chan [2]string)
	kafkaBroker := []string{"x:x"}

	cliOpt := mqtt.NewClientOptions()

	cliOpt.AddBroker("tcp://x:x").SetClientID("hjm_1538070623").SetUsername("HJM3")

	mqttCli := mqtt.NewClient(cliOpt)

	if token := mqttCli.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	message.AsyncProducer = producer.NewAsyncProducer(kafkaBroker)

	if token := mqttCli.Subscribe("go-mqtt/sample", 0, message.MessageHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for mqttCli.IsConnected() {

		receiveMsg := <-message.Channel

		fmt.Printf("接收到来自MQTT主题[%s]的消息, 内容为[%s]\n", receiveMsg[0], receiveMsg[1])
	}
}
