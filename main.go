package main

import (
	"fmt"
	"log_agent/commons"
	"log_agent/etcd"
	"log_agent/kafka"
	"log_agent/server"

	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

// func handler(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "test/plain")
// 	p := pprof.Lookup("goroutine")
// 	p.WriteTo(w, 1)
// }

func main() {
	// 0.读配置文件
	var iniConfig = new(commons.Config)
	err := ini.MapTo(iniConfig, "./conf/config.ini")
	if err != nil {
		logrus.Error("Load Config Failed :", err)
		return
	}

	// 1.初始化
	//// 1.1连接kafka
	err = kafka.InitKafka([]string{iniConfig.KafkaConfig.Address}, iniConfig.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Error("Kafka Connect Failed :", err)
		return
	}
	logrus.Info("Init Kafka Success")

	// 从etcd中获取日志的配置项
	etcd.Init([]string{iniConfig.EtcdConfit.Address})
	etcdConfig := etcd.GetConfig(iniConfig.EtcdConfit.CollectKey)
	fmt.Println(etcdConfig)

	// 2.把日志通过sarama发送到kafka
	err = server.Run(iniConfig, &etcdConfig)
	if err != nil {
		logrus.Errorf("server run failed :%v\n", err)
		return
	}
	for {
	}

	// go func() {
	// 	for {
	// 		select {
	// 		case <-time.After(time.Second):
	// 			fmt.Println("当前goroutine数量为:", runtime.NumGoroutine())
	// 		}
	// 	}
	// }()

	// http.HandleFunc("/", handler)
	// http.ListenAndServe(":8000", nil)
}
