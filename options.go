package bitcask_go

import "os"

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrite    bool
	IndexType    IndexerType
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1
	// adaptive radix tree 自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrite:    false,
	IndexType:    BTree,
}
