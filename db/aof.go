package db

import (
	"bufio"
	"io"
	"mygodis/lib/logger"
	"mygodis/redis/reply"

	"os"
	"strconv"
	"strings"
	"time"
)

var pExpireAtCmd = []byte("PEXPIREAT")

func makeExpireCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(args)
}

func makeAofCmd(cmd string, args [][]byte) *reply.MultiBulkReply {
	params := make([][]byte, len(args)+1)
	copy(params[1:], args)
	params[0] = []byte(cmd)
	return reply.MakeMultiBulkReply(params)
}
// listen aof channel and write into file
func (db *DB) handleAof() {
	for cmd := range db.aofChan {
		db.pausingAof.RLock() // prevent other goroutines from pausing aof
		if db.aofRewriteChan != nil {
			// replica during rewrite
			db.aofRewriteChan <- cmd
		}
		_, err := db.aofFile.Write(cmd.ToBytes())
		if err != nil {
			logger.Warnf(err.Error())
		}
		db.pausingAof.RUnlock()
	}
}

// read aof file
func (db *DB) loadAof(maxBytes int) {
	// delete aofChan to prevent write again
	aofChan := db.aofChan
	db.aofChan = nil
	defer func(aofChan chan *reply.MultiBulkReply) {
		db.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(db.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warnf(err.Error())
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var fixedLen int64 = 0
	var expectedArgsCount uint32
	var receivedCount uint32
	var args [][]byte
	processing := false
	var msg []byte
	readBytes := 0
	for {
		if maxBytes != 0 && readBytes >= maxBytes {
			break
		}
		if fixedLen == 0 {
			msg, err = reader.ReadBytes('\n')
			if err == io.EOF {
				return
			}
			if len(msg) == 0 {
				logger.Warnf("invalid format: line should end with \\r\\n")
				return
			}
			readBytes += len(msg)
		} else {
			msg = make([]byte, fixedLen+2)
			n, err := io.ReadFull(reader, msg)
			if err == io.EOF {
				return
			}
			if len(msg) == 0 {
				logger.Warnf("invalid multibulk length")
				return
			}
			fixedLen = 0
			readBytes += n
		}
		if err != nil {
			logger.Warnf(err.Error())
			return
		}

		if !processing {
			// new request
			if msg[0] == '*' {
				// bulk multi msg
				expectedLine, err := strconv.ParseUint(trim(msg[1:]), 10, 32)
				if err != nil {
					logger.Warnf(err.Error())
					return
				}
				expectedArgsCount = uint32(expectedLine)
				receivedCount = 0
				processing = true
				args = make([][]byte, expectedLine)
			} else {
				logger.Warnf("msg should start with '*'")
				return
			}
		} else {
			// receive following part of a request
			line := msg[0 : len(msg)-2]
			if line[0] == '$' {
				fixedLen, err = strconv.ParseInt(trim(line[1:]), 10, 64)
				if err != nil {
					logger.Warnf(err.Error())
					return
				}
				if fixedLen <= 0 {
					logger.Warnf("invalid multibulk length")
					return
				}
			} else {
				args[receivedCount] = line
				receivedCount++
			}

			// if sending finished
			if receivedCount == expectedArgsCount {
				processing = false

				cmd := strings.ToLower(string(args[0]))
				cmdFunc, ok := router[cmd]
				if ok {
					cmdFunc(db, args[1:])
				}

				// finish
				expectedArgsCount = 0
				receivedCount = 0
				args = nil
			}
		}
	}
}

func trim(msg []byte) string {
	trimed := ""
	for i := len(msg) - 1; i >= 0; i-- {
		if msg[i] == '\r' || msg[i] == '\n' {
			continue
		}
		return string(msg[:i+1])
	}
	return trimed
}
