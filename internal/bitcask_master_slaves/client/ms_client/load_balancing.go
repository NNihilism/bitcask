package msClient

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/log"
	"sync"
)

var loadBalancingMu = sync.Mutex{}

type weightInfo struct {
	weight int
	id     string
}

var currentWeight, effectiveWeight []weightInfo
var totalWeight int
var lastTime int64

func (cli *Client) getRpc(cmd string) nodeservice.Client {
	loadBalancingMu.Lock()
	defer loadBalancingMu.Unlock()

	if !isReadOperation(cmd) {
		return cli.masterRpc
	}
	slaveId := cli.selectNode()

	iInfo, ok := cli.nodesInfo.Load(slaveId)
	if !ok {
		log.Errorf("Load node[%s] info failed", slaveId)
		return nil
	}
	info, ok := iInfo.(nodeInfo)
	if !ok {
		log.Errorf("Convert iInfo(%v) to info failed", iInfo)
		return nil
	}
	return info.rpc
}

func (cli *Client) selectNode() string {
	// 选择一个合适的节点进行请求转发
	cli.mu.RLock()
	defer cli.mu.RUnlock()

	if cli.lastNodeUpdate > lastTime {
		cli.resetLoadBalancing()
	}

	return cli.updateLoadBalancing()
}

func (cli *Client) resetLoadBalancing() {
	currentWeight, effectiveWeight = []weightInfo{}, []weightInfo{}
	totalWeight = 0

	cli.nodesInfo.Range(func(id, iInfo interface{}) bool {
		info, ok := iInfo.(nodeInfo)
		if !ok {
			log.Errorf("convert iInfo [%v] to info failed", iInfo)
			return false
		}
		totalWeight += info.weight
		effectiveWeight = append(effectiveWeight, weightInfo{
			weight: info.weight,
			id:     info.id,
		})
		return true
	})
	currentWeight = make([]weightInfo, len(effectiveWeight))
	lastTime = cli.lastNodeUpdate
}

func (cli *Client) updateLoadBalancing() string {
	res_idx := 0
	for i := range effectiveWeight {
		currentWeight[i].weight += effectiveWeight[i].weight
		if currentWeight[i].weight > currentWeight[res_idx].weight {
			res_idx = i
		}
	}
	currentWeight[res_idx].weight -= totalWeight
	return currentWeight[res_idx].id
}

func isReadOperation(command string) bool {
	if _, ok := cmdReadOperationMap[command]; ok {
		return true
	}
	return false
}

var cmdReadOperationMap = map[string]struct{}{
	// string commands
	"get":      {},
	"mget":     {},
	"getrange": {},
	"strlen":   {},

	// list
	"llen":   {},
	"lindex": {},
	"lrange": {},

	// hash commands
	"hget":    {},
	"hmget":   {},
	"hexists": {},
	"hlen":    {},
	"hkeys":   {},
	"hvals":   {},
	"hgetall": {},
	"hstrlen": {},
	"hscan":   {},

	// set commands
	"sismember": {},
	"smembers":  {},
	"scard":     {},
	"sdiff":     {},
	"sunion":    {},

	// zset commands
	"zscore":    {},
	"zcard":     {},
	"zrange":    {},
	"zrevrange": {},
	"zrank":     {},
	"zrevrank":  {},
}
