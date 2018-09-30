package main

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"os"
)

var channel chan [2]string

func messageHandler(client mqtt.Client, message mqtt.Message){
	channel <- [2]string{message.Topic(), string(message.Payload())}
}

func main(){

	channel = make(chan [2]string)

	cliOpt:=mqtt.NewClientOptions()

	cliOpt.AddBroker("120.77.245.156:1883").SetClientID("hjm_1538070622").SetUsername("HJM")

	mqttCli := mqtt.NewClient(cliOpt)

	if token:=mqttCli.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := mqttCli.Subscribe("go-mqtt/sample", 0, messageHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for mqttCli.IsConnected(){

		receiveMsg := <-channel

		fmt.Printf("接收到来自主题[%s]的消息, 内容为[%s]\n",receiveMsg[0], receiveMsg[1])
	}
}