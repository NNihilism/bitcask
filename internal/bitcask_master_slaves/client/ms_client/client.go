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

type nodeInfo struct {
	addr string
	id   string
	rpc  nodeservice.Client

	weight int //权重
	// delay  int //往返时延
}

type Client struct {
	nodesInfo      sync.Map
	masterRpc      nodeservice.Client
	lastNodeUpdate int64
	mu             sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
}

type MSClientConfig struct {
	MasterHost string
	MasterPort string
}

func NewClient(cf *MSClientConfig) *Client {
	masterRpc := getNodeserviceClient(cf.MasterHost, cf.MasterPort)

	ctx, cancel := context.WithCancel(context.Background())
	cli := &Client{
		masterRpc:      masterRpc,
		lastNodeUpdate: 0,
		mu:             sync.RWMutex{},
		ctx:            ctx,
		cancel:         cancel,
	}

	go cli.updateNodeInfo(ctx, time.NewTicker(time.Second*5))

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

func (cli *Client) updateNodeInfo(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C: // 获取所有从节点信息
			resp, err := cli.masterRpc.GetAllNodesInfo(context.Background(), &node.GetAllNodesInfoReq{})
			if err != nil {
				log.Errorf("Get all nodes into err [%v]", err)
				return
			}

			// 节点没更新
			if cli.lastNodeUpdate == resp.LastUpdateTime {
				break
			}

			// 使用覆盖的形式 不上锁
			var tmpNodeInfo sync.Map
			for _, info := range resp.Infos {
				tmpRpc, err := nodeservice.NewClient(
					consts.NodeServiceName,
					client.WithHostPorts(info.Addr),
				)
				if err != nil {
					log.Errorf("Init slave rpc err [%v]", err)
					continue
				}
				tmpNodeInfo.Store(info.Id, nodeInfo{
					id:     info.Id,
					addr:   info.Addr,
					weight: int(info.Weight),
					rpc:    tmpRpc,
				})

			}
			cli.mu.Lock()
			cli.nodesInfo = tmpNodeInfo
			cli.lastNodeUpdate = resp.LastUpdateTime
			defer cli.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}

}
