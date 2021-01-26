package main

import (
	"mygodis/lib/logger"
	"mygodis/tcp"
)


func main() {
	logger.ConfigLocalFilesystemLogger("./ttt.log","debug")

	tcp.ListenAndServe(&tcp.Config{Address: ":8000"}, tcp.GetHandler())
}
