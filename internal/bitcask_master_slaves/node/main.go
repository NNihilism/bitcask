package main

import (
	user "bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"log"
	"net"

	"github.com/cloudwego/kitex/server"
)

func main() {
	addr, err := net.ResolveTCPAddr(consts.TCP, consts.NodeAddr)
	if err != nil {
		panic(err)
	}

	svr := user.NewServer(
		new(NodeServiceImpl),
		server.WithServiceAddr(addr),
	)

	err = svr.Run()

	if err != nil {
		log.Println(err.Error())
	}
}
