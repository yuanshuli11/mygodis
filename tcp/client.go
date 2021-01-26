package tcp

import (
	"bufio"
	"context"
	"io"
	"log"
	"mygodis/db"
	"mygodis/redis/reply"
	"mygodis/tools/easeatomic"
	"mygodis/tools/wait"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)
var (
	UnknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Client struct {
	//与客户端等tcp连接
	conn net.Conn
	/*
	 * 带有 timeout 功能的 WaitGroup, 用于优雅关闭
	 * 当响应被完整发送前保持 waiting 状态, 阻止链接被关闭
	 */
	waitingReply wait.Wait
	/* 标记客户端是否正在发送指令 */
	sending easeatomic.AtomicBool
	/* 客户端正在发送的参数数量, 即 Array 第一行指定的数组长度 */
	expectedArgsCount uint32
	/* 已经接收的参数数量， 即 len(args)*/
	receivedCount uint32
	/*
	 * 已经接收到的命令参数，每个参数由一个 []byte 表示
	 */
	args [][]byte
	// lock while server sending response
	mu sync.Mutex
}
func (c *Client)Write(b []byte)error {
	if b == nil || len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.conn.Write(b)
	return err
}

func (c *Client) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	c.conn.Close()
	return nil
}

type EchoHandler struct {
	/*
	 * 记录活跃的客户端链接
	 * 类型为 *Client -> placeholder
	 */
	activeConn sync.Map
	/* 数据库引擎，执行指令并返回结果 */
	db db.DB

	/* 关闭状态标志位，关闭过程中时拒绝新建连接和新请求 */
	closing easeatomic.AtomicBool
}
func GetHandler()*EchoHandler{
	handler := new(EchoHandler)
	db := db.MakeDB()
	handler.db = *db
	return handler
}
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		conn.Close()
	}
	client := &Client{
		conn: conn,
	}
	h.activeConn.Store(client, 1)
	reader := bufio.NewReader(conn)
	var (
		fixedLen int64
		err      error
		msg      []byte
	)
	for {

		if fixedLen == 0 {
			msg, err = reader.ReadBytes('\n')
			if len(msg) == 0 || msg[len(msg)-2] != '\r' {
				errReply := &reply.ProtocolErrReply{Msg:"invalid multibulk length"}
				client.conn.Write(errReply.ToBytes())
			}
		} else {
			msg = make([]byte, fixedLen+2)
			_, err = io.ReadFull(reader, msg)
			if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
				errReply := &reply.ProtocolErrReply{Msg:"invalid multibulk length"}
				client.conn.Write(errReply.ToBytes())
			}
			fixedLen = 0
		}
		//处理IO异常
		if err != nil {
			if err == io.EOF {
				log.Println("connection close")
				h.activeConn.Delete(conn)
			} else {
				log.Println(err)
			}
			client.Close()
			h.activeConn.Delete(client)
			return
		}

		if !client.sending.Get() {
			if msg[0] == '*' {
				expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					errReply := &reply.ProtocolErrReply{Msg: err.Error()}
					client.conn.Write(errReply.ToBytes())
					continue
				}
				client.waitingReply.Add(1)
				client.sending.Set(true)

				client.expectedArgsCount = uint32(expectedLine)
				client.receivedCount = 0
				client.args = make([][]byte, expectedLine)
			} else {
				// text protocol
				// remove \r or \n or \r\n in the end of line
				str := strings.TrimSuffix(string(msg), "\n")
				str = strings.TrimSuffix(str, "\r")
				strs := strings.Split(str, " ")
				args := make([][]byte, len(strs))
				for i, s := range strs {
					args[i] = []byte(s)
				}

				// send reply
				result := h.db.Exec(client, args)
				if result != nil {
					_ = client.Write(result.ToBytes())
				} else {
					_ = client.Write(UnknownErrReplyBytes)
				}
			}
		} else {
			line := msg[0 : len(msg)-2] //移除换行符
			if line[0] == '$' {
				fixedLen, err = strconv.ParseInt(string(line[1:1]), 10, 64)
				if err != nil {
					errReply := &reply.ProtocolErrReply{Msg: err.Error()}
					client.conn.Write(errReply.ToBytes())
				}
				if fixedLen <= 0 {
					errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
					client.conn.Write(errReply.ToBytes())
				}
			} else {
				client.args[client.receivedCount] = line
				client.receivedCount++
			}
			if client.receivedCount == client.expectedArgsCount {
				client.sending.Set(false)

				//执行命令并响应
				//TODO
				result :=h.db.Exec(client,client.args)
				client.Write(result.ToBytes())
				client.expectedArgsCount = 0
				client.receivedCount = 0
				client.args = nil
				client.waitingReply.Done()
			}
		}
	}
}
func (h *EchoHandler) Close() error {
	log.Println("handler shuting down...")
	h.closing.Set(true)

	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*Client)
		client.Close()
		return true
	})
	return nil
}
