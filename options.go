package bitcask_go

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
