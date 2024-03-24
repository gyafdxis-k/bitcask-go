package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) (*data.LogRecordPos, bool)

	Size() int
	Iterator(reverse bool) Iterator
	Close() error
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota + 1
	// ART 自适应基数树索引
	ART
	BPTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// 使用索引迭代器
type Iterator interface {
	Rewind()
	// 根据key 查找第一个大于或者小于key的元素
	Seek(key []byte)
	// 跳转到下一个key
	Next()
	Valid() bool
	Key() []byte
	Value() *data.LogRecordPos
	Close()
}
