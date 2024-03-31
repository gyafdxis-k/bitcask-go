package redis

import (
	bitcask_go "bitcask-go"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)
	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(val1))

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	t.Log(string(val2))

	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, bitcask_go.ErrKeyNotFound, err)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)
	t.Log(err)
	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)
	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)
	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask_go.ErrKeyNotFound, err)

}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	t.Log(ok1)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	t.Log(ok2)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(100))
	assert.Nil(t, err)
	t.Log(ok3)
	ok4, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	t.Log(string(ok4))
	ok5, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	t.Log(string(ok5))
}

func TestRedisDataStructure_Set(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-set")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	add, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, add)
	sAdd, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, !sAdd)

	sAdd, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, sAdd)

	member, err := rds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	t.Log(member)
	member, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	t.Log(member)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-set")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	add, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, add)
	sAdd, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, !sAdd)

	sAdd, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, sAdd)

	member, err := rds.SRem(utils.GetTestKey(2), []byte("val-1"))
	t.Log(member)
	member, err = rds.SRem(utils.GetTestKey(1), []byte("val-1"))
	t.Log(member)
}

func TestRedisDataStructure_LPush(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-lPush")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	add, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	t.Log(add)
	sAdd, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	t.Log(sAdd)

	sAdd, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	t.Log(sAdd)

	res, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(res))
	res, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(res))

	res, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(res))

}

func TestRedisDataStructure_ZScore(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-zset")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 98, []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	score, err := rds.ZScore(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(333), score)
	score, err = rds.ZScore(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(98), score)

}
