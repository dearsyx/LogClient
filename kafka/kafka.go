package kafka

import (
	"log_agent/tailutil"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type msgChan chan interface{}

var client sarama.SyncProducer

// var MsgChan chan *sarama.ProducerMessage

// 初始化全局kafka连接
func InitKafka(address []string, ChanSize int64) (err error) {
	// 创建配置文件
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 10

	// 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		return err
	}
	return nil
}

// msgChan构造函数
func newMsgChan(ChanSize int64) msgChan {
	return make(msgChan, ChanSize)
}

// 向msgChan中发送数据
func (m msgChan) SaveMsg(msg interface{}, topic string) {
	// 构建消息对象
	switch msg.(type) {
	case string:
		productMsg := &sarama.ProducerMessage{}
		productMsg.Topic = topic
		productMsg.Value = sarama.StringEncoder(msg.(string))
		// 发送消息
		m <- productMsg
	case bool:
		m <- msg
	}
}

// 从Tailtruct切片中不断获取数据发送到chan中
func (m msgChan) SaveGroupMsg(tailList *[]tailutil.TailStruct) {
	for _, tailObj := range *tailList {
		go func(t tailutil.TailStruct) {
			for {
				msg := <-t.TailChan
				switch msg.(type) {
				case string:
					m.SaveMsg(msg, t.Topic)
				case bool:
					m.SaveMsg(msg, t.Topic)
					return
				}
			}
		}(tailObj)
	}
}

// 实例化一个msgChan对象并开启一个SendMsg goroutine
func MakeChanAndSend(ChanSize int64) msgChan {
	m := newMsgChan(ChanSize)
	go func(m msgChan) {
		for {
			msg := <-m
			switch msg.(type) {
			case *sarama.ProducerMessage:
				pid, offset, err := client.SendMessage(msg.(*sarama.ProducerMessage))
				if err != nil {
					logrus.Error("kafka : send message failed :", err)
					return
				}
				logrus.Infof("send message to kafka success, pid:%v offset:%v", pid, offset)
			case bool:
				close(m)
				return
			}
		}
	}(m)
	return m
}