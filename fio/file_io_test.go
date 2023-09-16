package fio

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
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
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
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
