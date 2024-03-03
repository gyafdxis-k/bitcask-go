package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.Nil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_NewIterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)
	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	t.Log(string(iterator.Key()))
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	value, _ := iterator.Value()
	assert.Equal(t, utils.GetTestKey(10), value)
	// 47:17
}

func TestDB_NewIterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	t.Log(utils.GetTestKey(10))
	err = db.Put([]byte("aaaa"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("abaa"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aaba"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aaba"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aaab"), utils.GetTestKey(10))
	assert.Nil(t, err)
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
		t.Log("key=", string(iter1.Key()))
	}

	iter1.Rewind()
	for iter1.Seek([]byte("a")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
		t.Log("key=", string(iter1.Key()))
	}
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
		t.Log("key=", string(iter2.Key()))
	}

	// 指定prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("aa")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key=", string(iter3.Key()))
	}
}
