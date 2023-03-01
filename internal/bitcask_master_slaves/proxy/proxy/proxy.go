package proxy

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"sync"
	"time"
)

type Node struct {
	addr string
	id   string
	// 可以用于负载均衡决策
	weight int //权重
	// delay  int //往返时延
}

type Proxy struct {
	addr           string
	node           []Node
	slaveRpcs      map[string]nodeservice.Client
	masterRpc      nodeservice.Client
	lastNodeUpdate int64
	mu             sync.RWMutex
}

func NewProxy() *Proxy {
	return &Proxy{
		addr: consts.ProxyAddr,
		// id:   consts.ProxyAddr,
		lastNodeUpdate: time.Now().Unix(),
		node:           make([]Node, 0),
		slaveRpcs:      make(map[string]nodeservice.Client),
		mu:             sync.RWMutex{},
	}
}
