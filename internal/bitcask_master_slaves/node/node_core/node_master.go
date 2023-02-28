package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/log"
	"fmt"
	"time"

	"github.com/cloudwego/kitex/client"
)

func (bitcaskNode *BitcaskNode) HandleSlaveOfReq(req *node.RegisterSlaveRequest) (*node.RegisterSlaveResponse, error) {
	// 判断是否能够添加slave
	// 星型拓扑 非master节点不能添加slave
	if config.NodeTopology == config.Star && bitcaskNode.cf.Role != config.Master {
		return &node.RegisterSlaveResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_SlaveofErrCode),
				StatusMessage: fmt.Sprintf("Apply to be a slave node of a non-master node under topology [%s]", config.RoleNameMap[bitcaskNode.cf.Role]),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}
	// 判断是否重复添加
	if _, ok := bitcaskNode.getSlaveRPC(req.RunId); ok {
		// if _, ok := bitcaskNode.slavesRpc[req.RunId]; ok {
		return &node.RegisterSlaveResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_SlaveofErrCode),
				StatusMessage: fmt.Sprintf("Already a slave node of node [ip:% s, runid:% s].", bitcaskNode.cf.Addr, bitcaskNode.cf.ID),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	// 添加slave
	// 1. rpc初始化
	// if len(bitcaskNode.slavesRpc) == 0 {
	// 	bitcaskNode.slavesRpc = make(map[string]nodeservice.Client)
	// }

	c, err := nodeservice.NewClient(
		consts.NodeServiceName,
		client.WithHostPorts(req.Address),
	)
	if err != nil {
		log.Errorf("Init rpcclient err [%v]", err)
		return &node.RegisterSlaveResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_SlaveofErrCode),
				StatusMessage: fmt.Sprintf("Init rpcclient err [%v]", err),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}
	bitcaskNode.slavesRpc.Store(req.RunId, c)
	// bitcaskNode.slavesRpc[req.RunId] = c

	// 2. 修改变量
	bitcaskNode.cf.ConnectedSlaves += 1
	// if len(bitcaskNode.slavesStatus) == 0 {
	// bitcaskNode.slavesStatus = make(map[string]nodeSynctatusCode)
	// }
	bitcaskNode.slavesStatus.Store(req.RunId, nodeInIdle)
	// bitcaskNode.slavesStatus[req.RunId] = nodeInIdle
	// 返回结果
	return &node.RegisterSlaveResponse{
		BaseResp: &node.BaseResp{
			StatusCode:    int64(node.ErrCode_SuccessCode),
			StatusMessage: fmt.Sprintf("Slaveof node [ip:% s, runid:% s] successfully.", bitcaskNode.cf.Addr, bitcaskNode.cf.ID),
			ServiceTime:   time.Now().Unix(),
		},
	}, nil
}

func (bitcaskNode *BitcaskNode) changeSlaveSyncStatus(slaveId string, status nodeSynctatusCode) {
	if _, ok := bitcaskNode.getSlaveStatus(slaveId); !ok {
		bitcaskNode.RemoveSlave(slaveId)
		return
	}
	bitcaskNode.slavesStatus.Store(slaveId, status)
}

func (bitcaskNode *BitcaskNode) RemoveSlave(slaveId string) error {
	bitcaskNode.cf.ConnectedSlaves--
	// delete(bitcaskNode.slavesRpc, slaveId)
	// delete(bitcaskNode.slavesStatus, slaveId)
	bitcaskNode.slavesRpc.Delete(slaveId)
	bitcaskNode.slavesStatus.Delete(slaveId)
	return nil
}

// 缓存最小单元
// lru中存储的value需要实现方法Len(), *node.LogEntryRequest是由kitex生成，不应该对其进行修改，故再进行简单封装
type cacheItem struct {
	req *node.LogEntryRequest
}

func (item *cacheItem) Len() int {
	req := item.req
	var res int = 8 // req.EntryId int64 8Byte

	for i := range req.Args_ {
		res += len(req.Args_[i])
	}

	res += len(req.Cmd)

	return res
}

func (bitcaskNode *BitcaskNode) AddCache(req *node.LogEntryRequest) {
	bitcaskNode.cacheMu.Lock()
	defer bitcaskNode.cacheMu.Unlock()

	bitcaskNode.opCache.Add(fmt.Sprintf("%d", req.EntryId), &cacheItem{req: req})
}

func (bitcaskNode *BitcaskNode) GetCache(key int64) (*node.LogEntryRequest, bool) {
	bitcaskNode.cacheMu.Lock()
	defer bitcaskNode.cacheMu.Unlock()

	if bitcaskNode.opCache == nil {
		return nil, false
	}

	if item, ok := bitcaskNode.opCache.Get(fmt.Sprintf("%d", key)); ok {
		return item.(*cacheItem).req, true
	}
	return nil, false
}

func (bitcaskNode *BitcaskNode) getSlaveRPC(slaveId string) (nodeservice.Client, bool) {
	iRpc, ok := bitcaskNode.slavesRpc.Load(slaveId)
	if !ok {
		log.Errorf("Get slave rpc [%s] failed", slaveId)
		bitcaskNode.RemoveSlave(slaveId)
		return nil, false
	}

	rpc, ok := iRpc.(nodeservice.Client)
	if !ok {
		log.Errorf("Convert nodeservice.Client err")
		bitcaskNode.RemoveSlave(slaveId)
		return nil, false
	}
	return rpc, true
}

func (bitcaskNode *BitcaskNode) getSlaveStatus(slaveId string) (nodeSynctatusCode, bool) {
	status, ok := bitcaskNode.slavesRpc.Load(slaveId)
	if !ok {
		log.Errorf("Get slave status [%s] failed", slaveId)
		bitcaskNode.RemoveSlave(slaveId)
		return nodeInIdle, false
	}
	return status.(nodeSynctatusCode), true
}
