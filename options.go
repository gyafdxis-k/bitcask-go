package bitcask_go

import "os"

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrite    bool
	// 累计写到多少持久化
	BytePerSync uint
	IndexType   IndexerType
	// 启动时是否加载mmap
	MMapAtStartup bool

	// 数据文件合并的阈值
	DataFileMergeRatio float32
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchNum uint
	SyncWrites  bool
}
type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1
	// adaptive radix tree 自适应基数树索引
	ART
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	SyncWrite:          false,
	BytePerSync:        0,
	IndexType:          BTree,
	MMapAtStartup:      false,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  false,
}
