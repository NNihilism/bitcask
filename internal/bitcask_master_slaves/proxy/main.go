package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	prxyservice "bitcaskDB/internal/bitcask_master_slaves/proxy/kitex_gen/prxyService/proxyservice"
	"log"
	"net"

	"github.com/cloudwego/kitex/server"
)

func init() {

}
func main() {
	addr, err := net.ResolveTCPAddr(consts.TCP, consts.ProxyAddr)
	if err != nil {
		panic(err)
	}

	// svr := user.NewServer(
	// 	new(NodeServiceImpl),
	// 	server.WithServiceAddr(addr),
	// )

	svr := prxyservice.NewServer(new(ProxyServiceImpl),
		server.WithServiceAddr(addr),
	)

	err = svr.Run()

	if err != nil {
		log.Println(err.Error())
	}
}
