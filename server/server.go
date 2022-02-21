package server

import (
	"log_agent/commons"
	"log_agent/etcd"
	"log_agent/kafka"
	"log_agent/tailutil"
)

// 获取日志文件中的日志并发送到kafka
func Run(iniConfig *commons.Config, etcdConfig *[]commons.CollectEntry) (err error) {
	tailList, err := tailutil.InitGroup(etcdConfig)
	for _, tailObj := range tailList {
		// 对每一个tail object开启一个goroutine从日志中读取数据并发送到TailChan中
		tailutil.GetMsgGoroutine(tailObj)
	}
	// 生成一个kafka的msgChan向kafka中发送数据
	msgChan := kafka.MakeChanAndSend(iniConfig.ChanSize)
	msgChan.SaveGroupMsg(&tailList)
	// 新建一个goroutine去监控CollectKey中的变化
	go etcd.WatchConfig(iniConfig.EtcdConfit.CollectKey, &tailList, iniConfig)
	return
}
