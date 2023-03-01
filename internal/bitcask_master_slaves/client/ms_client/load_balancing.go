package msClient

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"time"
)

var currentWeight, effectiveWeight []int
var totalWeight int
var lastTime int64

func (cli *Client) getRpc(cmd string) nodeservice.Client {
	if !isReadOperation(cmd) {
		return cli.masterRpc
	}
	slaveId := cli.selectNode()
	return cli.slaveRpcs[slaveId]
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
	currentWeight = make([]int, len(cli.node))
	effectiveWeight = make([]int, len(cli.node))
	totalWeight = 0
	for i, info := range cli.node {
		effectiveWeight[i] = info.weight
		totalWeight += info.weight
	}

	lastTime = time.Now().Unix()
}

func (cli *Client) updateLoadBalancing() string {
	res_idx := 0
	for i := range effectiveWeight {
		currentWeight[i] += effectiveWeight[i]
		if currentWeight[i] > currentWeight[res_idx] {
			res_idx = i
		}
	}
	currentWeight[res_idx] -= totalWeight
	return cli.node[res_idx].id
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
