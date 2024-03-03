package main

import (
	bitcask_go "bitcask-go"
	"fmt"
	"io/ioutil"
	"os"
)

func main() {
	testDataPath := "/Users/gaodong/workspace/go_workspace/kv-project/bitcask-go/test"
	_, err := os.Stat(testDataPath)
	if err != nil {
		panic(err)
	}
	files, err := ioutil.ReadDir(testDataPath)
	if len(files) != 0 {
		err = os.RemoveAll(testDataPath)
		if err != nil {
			panic(err)
			return
		}
	}
	err = os.Chmod(testDataPath, 0777)
	if err != nil {
		return
	}

	opts := bitcask_go.DefaultOptions
	opts.DirPath = testDataPath
	db, err := bitcask_go.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val=", string(val))
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

}
