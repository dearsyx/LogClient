package commons

// 收集指定目录下的日志文件，发送到kafka
// LogAgent的配置结构体
type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"msg_chansize"`
}

// Tail收集日志的目录
type CollectConfig struct {
	LogPath string `ini:"log_path"`
}

// etcd配置项
type EtcdConfit struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

//总配置
type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfit    `ini:"etcd"`
}

// 从etcd中读取的用于tail收集日志的配置项
type CollectEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}
