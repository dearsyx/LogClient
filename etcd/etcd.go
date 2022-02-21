package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log_agent/commons"
	"log_agent/kafka"
	"log_agent/tailutil"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/sirupsen/logrus"
)

// 获取etcd日志配置信息
var (
	client *clientv3.Client
	err    error
)

func Init(address []string) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Println("etcd package: Connect to etcd failed :", err)
		return
	}
	return
}

// 拉取日志收集配置项的函数
func GetConfig(key string) (confList []commons.CollectEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	getResp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("Get config from etcd error : %v, key:%s\n", err, key)
	}
	if len(getResp.Kvs) == 0 {
		logrus.Warningf("Get len:0 from etcd where key:%s\n", key)
		return
	}
	ret := getResp.Kvs[0].Value
	err = json.Unmarshal(ret, &confList)
	if err != nil {
		logrus.Errorf("JSON Unmarshal failed, err:%v\n", err)
	}
	return
}

// 监控etcd中key对应value的变化
func WatchConfig(key string, tailList *[]tailutil.TailStruct, iniConfig *commons.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	// 捕获到了变化return时，关闭watchChan
	defer cancel()
	watchChan := client.Watch(ctx, key)
	newConfig := []commons.CollectEntry{}
	for wResp := range watchChan {
		for _, event := range wResp.Events {
			logrus.Warningf("Etcd collect key has been changed, type:%s, key:%s, value:%s\n", event.Type, event.Kv.Key, event.Kv.Value)
			// 解析新value为config数据格式
			err := json.Unmarshal(event.Kv.Value, &newConfig)
			if err != nil {
				logrus.Errorf("Json unmarshal failed : %v\n", err)
				return
			}
		}

		// 停掉正在运行的tail goroutine
		for _, item := range *tailList {
			item.CloseChan <- struct{}{}
			// item.Tails.Cleanup()
			item.Tails.Stop()
		}
		// 启动新的tail goroutine
		tailList, err := tailutil.InitGroup(&newConfig)
		if err != nil {
			logrus.Errorf("start tails group failed, err: %v \n", err)
			return
		}
		logrus.Infof("value of key:%s in etcd has been changed, restart goroutine to listen log\n", key)
		for _, tailObj := range tailList {
			// 对每一个tail object开启一个goroutine从日志中读取数据并发送到TailChan中
			tailutil.GetMsgGoroutine(tailObj)
		}
		// 生成一个kafka的msgChan向kafka中发送数据
		msgChan := kafka.MakeChanAndSend(iniConfig.ChanSize)
		msgChan.SaveGroupMsg(&tailList)
		// 新建一个goroutine去监控CollectKey中的变化
		go WatchConfig(iniConfig.EtcdConfit.CollectKey, &tailList, iniConfig)
		return
	}
}
