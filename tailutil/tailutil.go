package tailutil

import (
	"log_agent/commons"

	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

type TailStruct struct {
	// Tails 用于保存tail初始化得到的结构体
	Tails *tail.Tail

	// TailChan 用于存储从日志文件中读取到的日志 也用于指示向kafka发送消息的goroutine的关闭
	TailChan chan interface{}

	// 该tail对象对应的topic
	Topic string

	// CloseChan 用于判断当前正在读取日志的tail goroutine是否该关闭
	CloseChan chan struct{}
}

func InitGroup(iniConfig *[]commons.CollectEntry) (tailList []TailStruct, err error) {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	for _, item := range *iniConfig {
		tails, err := tail.TailFile(item.Path, config)
		if err != nil {
			return nil, err
		}
		tailList = append(tailList, TailStruct{tails, make(chan interface{}, 10), item.Topic, make(chan struct{})})
	}
	return
}

// 开启一个goroutine保持监控文件
func GetMsgGoroutine(t TailStruct) {
	// t.GetMsg()
	go func(t TailStruct) {
		for {
			select {
			// 如果CloseChan中有东西则该goroutine直接返回
			case <-t.CloseChan:
				// 指示该tail对应的向kafka发送消息的goroutine关闭
				t.TailChan <- true
				logrus.Infof("tailObj recive message from CloseChan, goroutine return\n")
				return
			case line, ok := <-t.Tails.Lines:
				if !ok {
					logrus.Warningf("file %s read from tail failed, return\n", t.Tails.Filename)
					return
				}
				if len(line.Text) == 0 {
					continue
				}
				t.TailChan <- line.Text
			}
		}
	}(t)
}
