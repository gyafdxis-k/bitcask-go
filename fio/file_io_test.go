package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func deStoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer deStoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer deStoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	n, err := fio.Write([]byte(""))
	assert.Equal(t, n, 0)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("bitcast-go kv"))
	t.Log(n, err)
	assert.Equal(t, 13, n)
	n, err = fio.Write([]byte("storage"))
	t.Log(n, err)
	assert.Equal(t, 7, n)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer deStoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b)
	t.Log(b, n)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, []byte("key-b"), b2)
	t.Log(b2, n)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer deStoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "0002.data")
	fio, err := NewFileIOManager(path)
	defer deStoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	err = fio.Close()
	assert.Nil(t, err)
}
