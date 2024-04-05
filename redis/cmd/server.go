package main

import (
	bitcask_go "bitcask-go"
	"bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6378"

type BitcaskServer struct {
	dbs    map[int]*redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func (svr *BitcaskServer) listen() {
	log.Print("bitcask server is runing listening 6378")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

func main() {
	redisDataStructure, err := redis.NewRedisDataStructure(bitcask_go.DefaultOptions)
	if err != nil {
		panic(err)
	}
	bitcaskServer := BitcaskServer{
		dbs: make(map[int]*redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure
	server := redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.server = server
	bitcaskServer.listen()

}
