package db

import (
	"mygodis/datastruct/dict"
	"mygodis/interface/redis"
	"mygodis/lib/logger"
	"mygodis/redis/reply"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	ttlDictSize  = 1 << 10
	aofQueueSize = 1 << 16
)

var router = GetRouter()

type DataEntity struct {
	Data interface{}
}
type DB struct {
	// key -> expireTime (time.Time)
	Data      dict.Dict
	TTLMap    dict.Dict
	stopWorld sync.WaitGroup

	// 主线程使用此channel将要持久化的命令发送到异步协程
	aofChan chan *reply.MultiBulkReply
	// append file 文件描述符
	aofFile        *os.File
	aofFilename    string
	aofRewriteChan chan *reply.MultiBulkReply
	// 在必要的时候使用此字段暂停持久化操作
	pausingAof sync.RWMutex
}

func MakeDB() *DB {
	db := &DB{
		Data:   dict.MakeConcurrent(ttlDictSize),
		TTLMap: dict.MakeConcurrent(ttlDictSize),
	}
	db.aofFilename = "appendonly.aof"
	db.loadAof(0)
	aofFile, err := os.OpenFile(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		logger.Warnf(err.Error())
	} else {
		db.aofFile = aofFile
		db.aofChan = make(chan *reply.MultiBulkReply, aofQueueSize)
	}
	go func() {
		db.handleAof()
	}()
	return db
}
func (db *DB) Persist(key string) {
	//db.stopWorld.Wait()
	db.TTLMap.Remove(key)
}

func (db *DB) Exec(client redis.Connection, args [][]byte) (result reply.Reply) {
	defer func() {
		if err := recover(); err != nil {
			result = &reply.UnknownErrReply{}
			return
		}
	}()
	cmd := strings.ToLower(string(args[0]))

	cmdFunc, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
	}
	if len(args) > 1 {
		result = cmdFunc(db, args[1:])
	} else {
		result = cmdFunc(db, [][]byte{})
	}

	return
}

func (db *DB) Put(key string, entity *DataEntity) int {
	//db.stopWorld.Wait()
	return db.Data.Put(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *DataEntity) int {
	//db.stopWorld.Wait()
	return db.Data.PutIfAbsent(key, entity)
}

func (db *DB) Expire(key string, expireTime time.Time) {
	//db.stopWorld.Wait()
	db.TTLMap.Put(key, expireTime)
}

func (db *DB) PutIfExists(key string, entity *DataEntity) int {
	//	db.stopWorld.Wait()
	return db.Data.PutIfExists(key, entity)
}

func (db *DB) Get(key string) (*DataEntity, bool) {
	db.stopWorld.Wait()

	raw, ok := db.Data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.TTLMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

func (db *DB) Remove(key string) {
	db.stopWorld.Wait()
	db.Data.Remove(key)
	db.TTLMap.Remove(key)
}

// send command to aof
func (db *DB) AddAof(args *reply.MultiBulkReply) {
	// aofChan == nil when loadAof
	//if config.Properties.AppendOnly && db.aofChan != nil {
	if db.aofChan != nil {
		db.aofChan <- args
	}
}
