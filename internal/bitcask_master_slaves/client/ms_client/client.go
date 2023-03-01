package msClient

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/log"
	"context"
	"sync"
	"time"

	"github.com/cloudwego/kitex/client"
)

type Node struct {
	addr string
	id   string
	// 可以用于负载均衡决策
	weight int //权重
	// delay  int //往返时延
}

type Client struct {
	// addr           string
	node           []Node
	slaveRpcs      map[string]nodeservice.Client
	masterRpc      nodeservice.Client
	lastNodeUpdate int64
	mu             sync.RWMutex
}

type MSClientConfig struct {
	MasterHost string
	MasterPort string
}

func NewClient(cf *MSClientConfig) *Client {
	masterRpc := getNodeserviceClient(cf.MasterHost, cf.MasterPort)

	cli := &Client{
		masterRpc:      masterRpc,
		lastNodeUpdate: time.Now().Unix(),
		mu:             sync.RWMutex{},
		slaveRpcs:      make(map[string]nodeservice.Client),
	}
	// 获取所有从节点信息
	resp, err := masterRpc.GetAllNodesInfo(context.Background(), &node.GetAllNodesInfoReq{})
	if err != nil {
		log.Errorf("Get all nodes into err [%v]", err)
		return nil
	}

	// 初始化所有SlaveRPC
	for _, info := range resp.Infos {

		// for i := 0; i < len(resp.SlaveAddress); i++ {
		tmpRpc, err := nodeservice.NewClient(
			consts.NodeServiceName,
			client.WithHostPorts(info.Addr),
		)
		if err != nil {
			log.Errorf("Init slave rpc err [%v]", err)
			continue
		}
		cli.slaveRpcs[info.Id] = tmpRpc
		cli.node = append(cli.node, Node{
			addr:   info.Addr,
			id:     info.Addr,
			weight: int(info.Weight),
		})
	}

	return cli
}

func getNodeserviceClient(host, port string) nodeservice.Client {
	c, err := nodeservice.NewClient(
		consts.NodeServiceName,
		client.WithHostPorts(host+":"+port),
	)
	if err != nil {
		log.Errorf("Init master rpc err [%v]", err)
		return nil
	}
	return c
}

// func (cli *Client) Cmd(cmd []byte, args [][]byte)
