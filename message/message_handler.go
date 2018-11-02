package message

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/eclipse/paho.mqtt.golang"
)

const (
	STATUS_NORMAL = iota
	STATUS_WARN
	STATUS_ALARM
	STATUS_OFFLINE
	STATUS_ERROR
)

var Channel chan [2]string
var AsyncProducer sarama.AsyncProducer

type DeviceMessage struct {
	DeviceID     int64                    `json:"ID"`
	MsgTime      string                   `json:"once"`
	DeviceStatus int8                     `json:"status"`
	DeviceSecret string                   `json:"Sign,omitempty"`
	DeviceData   []map[string]interface{} `json:"ProbeData"`
}

func (dMsg *DeviceMessage) Verify() error {
	//todo
	return nil
}

func (dMsg *DeviceMessage) ToKafkaMsg() ([]byte, error) {
	//todo
	// not need to save secret, set it to empty value so will be omitted
	dMsg.DeviceSecret = ""
	return json.Marshal(dMsg)
}

func MessageHandler(client mqtt.Client, message mqtt.Message) {

	Channel <- [2]string{message.Topic(), string(message.Payload())}

	dMsg := DeviceMessage{}

	err := json.Unmarshal(message.Payload(), &dMsg)
	if err != nil {
		fmt.Println(err)
	}

	if valid := dMsg.Verify(); valid != nil {
		fmt.Printf("Invalid Device %d with secret %s", dMsg.DeviceID, dMsg.DeviceSecret)
		return
	}

	msg, err := dMsg.ToKafkaMsg()
	if err != nil {
		fmt.Println(err)
		return
	}

	AsyncProducer.Input() <- &sarama.ProducerMessage{
		Topic: "test",
		Key:   sarama.StringEncoder("test"),
		Value: sarama.StringEncoder(msg),
	}

	// 如果设备告警，写多一份入alarm队列
	if dMsg.DeviceStatus != STATUS_NORMAL {
		AsyncProducer.Input() <- &sarama.ProducerMessage{
			Topic: "alarm",
			Key:   sarama.StringEncoder("test"),
			Value: sarama.StringEncoder(msg),
		}
	}
}
