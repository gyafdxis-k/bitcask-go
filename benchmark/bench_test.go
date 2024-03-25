package benchmark

import (
	bitcask_go "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"
)

var db bitcask_go.DB

func init() {
	// 初始化存储引擎
	options := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.DirPath = dir
	var err error
	db, err := bitcask_go.Open(options)
	if err != nil {
		return
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err, _ := db.Put(utils.GetTestKey(i), utils.RandomValue(i))
	}
}
