package db

import (
	"mygodis/redis/reply"
	"strconv"
	"strings"
	"time"
)

const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)
const unlimitedTTL int64 = 0

func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.Get(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

func Get(db *DB, args [][]byte) reply.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'get' command")
	}
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &reply.NullBulkReply{}
	}
	return reply.MakeBulkReply(bytes)
}

/*
	args[0]  key
	args[1]  value
	args[2]  [EX seconds] [PX milliseconds] [NX|XX]
    SET key value [NX] [XX] [KEEPTTL] [GET] [EX <seconds>] [PX <milliseconds>]
*/
func Set(db *DB, args [][]byte) reply.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'set' command")
	}

	key := string(args[0])
	value := args[1]
	policy := upsertPolicy
	ttl := unlimitedTTL

	// parse options
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToUpper(string(args[i]))
			if arg == "NX" { // insert
				if policy == updatePolicy {
					return &reply.SyntaxErrReply{}
				}
				policy = insertPolicy
			} else if arg == "XX" { // update policy
				if policy == insertPolicy {
					return &reply.SyntaxErrReply{}
				}
				policy = updatePolicy
			} else if arg == "EX" { // ttl in seconds
				if ttl != unlimitedTTL {
					// ttl has been set
					return &reply.SyntaxErrReply{}
				}
				if i+1 >= len(args) {
					return &reply.SyntaxErrReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &reply.SyntaxErrReply{}
				}
				if ttlArg <= 0 {
					return reply.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg * 1000
				i++ // skip next arg
			} else if arg == "PX" { // ttl in milliseconds
				if ttl != unlimitedTTL {
					return &reply.SyntaxErrReply{}
				}
				if i+1 >= len(args) {
					return &reply.SyntaxErrReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &reply.SyntaxErrReply{}
				}
				if ttlArg <= 0 {
					return reply.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg
				i++ // skip next arg
			} else {
				return &reply.SyntaxErrReply{}
			}
		}
	}

	entity := &DataEntity{
		Data: value,
	}

	db.Persist(key) // clean ttl
	var result int
	switch policy {
	case upsertPolicy:
		result = db.Put(key, entity)
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}

	if ttl != unlimitedTTL {
		expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
		db.Expire(key, expireTime)
		db.AddAof(reply.MakeMultiBulkReply([][]byte{
			[]byte("SET"),
			args[0],
			args[1],
		}))
		db.AddAof(makeExpireCmd(key, expireTime))
	} else if result > 0 {
		db.Persist(key) // override ttl
		db.AddAof(makeAofCmd("set", args))
	}

	if policy == upsertPolicy || result > 0 {
		return &reply.OkReply{}
	} else {
		return &reply.NullBulkReply{}
	}
	return nil
}
