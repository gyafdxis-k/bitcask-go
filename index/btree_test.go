package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res2)

	res2 = bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, _ := bt.Delete(nil)
	assert.NotNil(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	res4, _ := bt.Delete([]byte("aaa"))
	assert.NotNil(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt := NewBTree()
	// 1 btree 为空
	iter1 := bt.Iterator(true)
	t.Log(iter1.Valid())
	assert.Equal(t, false, iter1.Valid())

	// 2 btree 有数据的情况
	bt.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt.Iterator(false)
	t.Log(iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	assert.Equal(t, true, iter2.Valid())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	bt.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("eee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("cce"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key = ", string(iter3.Key()))
		assert.NotNil(t, iter3.Value())
	}
	iter4 := bt.Iterator(false)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		t.Log("key = ", string(iter4.Key()))
		assert.NotNil(t, iter4.Value())
	}

	// 测试 seek
	iter5 := bt.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		t.Log(string(iter5.Key()))
		assert.NotNil(t, iter5.Key())
	}

	iter6 := bt.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		t.Log(string(iter6.Key()))
	}
}
