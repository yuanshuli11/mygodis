package db

import (
	"mygodis/redis/reply"
)

type CmdFunc func(db *DB, args [][]byte) reply.Reply


func Ping(db *DB, args [][]byte) reply.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply("\"" + string(args[0]) + "\"")
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}