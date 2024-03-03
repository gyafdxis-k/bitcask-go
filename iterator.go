package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// 用户 Iterator
type Iterator struct {
	indexIter index.Iterator
	db        *DB
	option    IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		option:    opts,
	}

}

func (iter *Iterator) Rewind() {
	iter.indexIter.Rewind()
	iter.skipToNext()
}

// 根据key 查找第一个大于或者小于key的元素
func (iter *Iterator) Seek(key []byte) {
	iter.indexIter.Seek(key)
	iter.skipToNext()
}

// 跳转到下一个key
func (iter *Iterator) Next() {
	iter.indexIter.Next()
	iter.skipToNext()
}

func (iter *Iterator) Valid() bool {
	return iter.indexIter.Valid()
}

func (iter *Iterator) Key() []byte {
	return iter.indexIter.Key()
}

func (iter *Iterator) Value() ([]byte, error) {
	logRecordPos := iter.indexIter.Value()
	iter.db.mu.RLock()
	defer iter.db.mu.RUnlock()
	return iter.db.getValueByPosition(logRecordPos)
}

func (iter *Iterator) Close() {
	iter.indexIter.Close()
}

func (iter *Iterator) skipToNext() {
	prefixLen := len(iter.option.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; iter.indexIter.Valid(); iter.indexIter.Next() {
		key := iter.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(iter.option.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
