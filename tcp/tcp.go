package tcp

import (
	"context"
	"fmt"
	"log"
	"mygodis/tools/easeatomic"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

func ListenAndServe(cfg *Config, handler Handler) {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		log.Fatal(fmt.Sprintf("listen err:%v", err))
	}
	log.Println("tcp begin",cfg.Address)
	var closing easeatomic.AtomicBool
	sigCh := make(chan os.Signal, 1)
	//也监听了ctrl-C
	signal.Notify(sigCh,os.Interrupt, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINFO)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT,os.Interrupt:
			log.Println("shuting down...")
			closing.Set(true)
			listener.Close()
			handler.Close()
		}
	}()

	defer log.Println(fmt.Sprintf("bind:%s,start listening...", cfg.Address))

	defer func() {
		listener.Close()
		handler.Close()
	}()
	ctx, _ := context.WithCancel(context.Background())

	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				log.Println("wait disconnect....")
				waitDone.Wait()
				return
			}
			log.Println("accept err", err)
		}
		log.Println("accept link")
		go func() {
			defer func() {
				waitDone.Done()
			}()
			waitDone.Add(1)
			handler.Handle(ctx, conn)
		}()
	}
}
