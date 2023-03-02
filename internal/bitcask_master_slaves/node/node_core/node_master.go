package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/log"
	"context"
	"fmt"
	"sync/atomic"
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
	if _, ok := bitcaskNode.slavesInfo.Load(req.RunId); ok {
		return &node.RegisterSlaveResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_SlaveofErrCode),
				StatusMessage: fmt.Sprintf("Already a slave node of node [ip:% s, runid:% s].", bitcaskNode.cf.Addr, bitcaskNode.cf.ID),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	// 1. rpc初始化
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

	bitcaskNode.slavesInfo.Store(req.RunId, slaveInfo{
		address: req.Address,
		id:      req.RunId,
		status:  nodeInIdle,
		weight:  int(req.Weight),
		rpc:     c,
	})
	bitcaskNode.infosLastUpdateTime = time.Now().Unix()
	atomic.AddInt32(&bitcaskNode.cf.ConnectedSlaves, 1)

	// 返回结果
	return &node.RegisterSlaveResponse{
		BaseResp: &node.BaseResp{
			StatusCode:    int64(node.ErrCode_SuccessCode),
			StatusMessage: fmt.Sprintf("Slaveof node [ip:% s, runid:% s] successfully.", bitcaskNode.cf.Addr, bitcaskNode.cf.ID),
			ServiceTime:   time.Now().Unix(),
		},
		RunId: bitcaskNode.cf.ID,
	}, nil
}

// sync.Map好像并不能满足这里的并发需求
// 虽然Load和Store是并发安全的，但是并不能原子执行
// 需要一个类似CAS的语句来保证并发安全
func (bitcaskNode *BitcaskNode) casSlaveSyncStatus(slaveId string, origin, target nodeSynctatusCode) bool {
	bitcaskNode.slaveInfoMu.Lock()
	defer bitcaskNode.slaveInfoMu.Unlock()

	iInfo, ok := bitcaskNode.slavesInfo.Load(slaveId)
	if !ok {
		log.Errorf("Get slave status [%s] failed", slaveId)
		bitcaskNode.RemoveSlave(slaveId)
		return false
	}
	info, ok := iInfo.(slaveInfo)
	if !ok {
		log.Errorf("Convert iInfo[%v] to info failed", iInfo)
		bitcaskNode.RemoveSlave(slaveId)
		return false
	}
	if origin != info.status {
		return false
	}

	info.status = target
	bitcaskNode.slavesInfo.Store(slaveId, info)

	return true
}

func (bitcaskNode *BitcaskNode) changeSlaveSyncStatus(slaveId string, status nodeSynctatusCode) bool {
	bitcaskNode.slaveInfoMu.Lock()
	defer bitcaskNode.slaveInfoMu.Unlock()

	iInfo, ok := bitcaskNode.slavesInfo.Load(slaveId)
	if !ok {
		log.Errorf("Get slave status [%s] failed", slaveId)
		bitcaskNode.RemoveSlave(slaveId)
		return false
	}
	info, ok := iInfo.(slaveInfo)
	if !ok {
		log.Errorf("Convert iInfo[%v] to info failed", iInfo)
		bitcaskNode.RemoveSlave(slaveId)
		return false
	}
	info.status = status
	bitcaskNode.slavesInfo.Store(slaveId, info)

	return true
}

func (bitcaskNode *BitcaskNode) RemoveSlave(slaveId string) error {
	bitcaskNode.slaveInfoMu.Lock()
	defer bitcaskNode.slaveInfoMu.Unlock()

	bitcaskNode.cf.ConnectedSlaves--
	bitcaskNode.slavesInfo.Delete(slaveId)
	bitcaskNode.infosLastUpdateTime = time.Now().Unix()

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

	// log.Info("添加缓存[%v]", req)
	bitcaskNode.replBakBuffer.Add(fmt.Sprintf("%d", req.EntryId), &cacheItem{req: req})
}

func (bitcaskNode *BitcaskNode) GetCache(key int64) (*node.LogEntryRequest, bool) {
	bitcaskNode.cacheMu.Lock()
	defer bitcaskNode.cacheMu.Unlock()

	if bitcaskNode.replBakBuffer == nil {
		return nil, false
	}

	if item, ok := bitcaskNode.replBakBuffer.Get(fmt.Sprintf("%d", key)); ok {
		return item.(*cacheItem).req, true
	}
	return nil, false
}

func (bitcaskNode *BitcaskNode) getSlaveRPC(slaveId string) (nodeservice.Client, bool) {
	iInfo, ok := bitcaskNode.slavesInfo.Load(slaveId)
	if !ok {
		log.Errorf("Get slave status [%s] failed", slaveId)
		bitcaskNode.RemoveSlave(slaveId)
		return nil, false
	}
	info, ok := iInfo.(slaveInfo)
	if !ok {
		log.Errorf("Convert iInfo[%v] to info failed", iInfo)
		bitcaskNode.RemoveSlave(slaveId)
		return nil, false
	}

	return info.rpc, true
}

func (bitcaskNode *BitcaskNode) getSlaveStatus(slaveId string) (nodeSynctatusCode, bool) {
	iInfo, ok := bitcaskNode.slavesInfo.Load(slaveId)
	if !ok {
		log.Errorf("Get slave status [%s] failed", slaveId)
		bitcaskNode.RemoveSlave(slaveId)
		return nodeInIdle, false
	}
	info, ok := iInfo.(slaveInfo)
	if !ok {
		log.Errorf("Convert iInfo[%v] to info failed", iInfo)
		bitcaskNode.RemoveSlave(slaveId)
		return nodeInIdle, false
	}

	return info.status, true
}

func (bitcaskNode *BitcaskNode) saveMasterConfig() {
	m := config.MasterConfigMap
	// 写入cur_offset
	bitcaskNode.db.HSet([]byte(m["key"]), []byte(m["field_cur_offset"]), []byte(fmt.Sprintf("%d", bitcaskNode.cf.CurReplicationOffset)))
	// 还可以补充别的?
}

func (bitcaskNode *BitcaskNode) GetAllNodesInfo(req *node.GetAllNodesInfoReq) (*node.GetAllNodesInfoResp, error) {
	// slavesAddr := []string{}
	// slavesId := []string{}
	bitcaskNode.slaveInfoMu.Lock()
	defer bitcaskNode.slaveInfoMu.Unlock()

	infos := []*node.SlaveInfo{}

	bitcaskNode.slavesInfo.Range(func(id, iInfo interface{}) bool {
		info, ok := iInfo.(slaveInfo)
		if !ok {
			log.Errorf("Convert iInfo[%v] to info failed", iInfo)
			return true
		}
		infos = append(infos, &node.SlaveInfo{
			Addr:   info.address,
			Id:     info.id,
			Weight: int32(info.weight),
		})
		return true
	})

	return &node.GetAllNodesInfoResp{
		Infos: infos,
	}, nil
}

func (bitcaskNode *BitcaskNode) checkSlavesAlive(ctx context.Context, ticker time.Ticker) {
	for {
		select {
		case <-ticker.C: // 非星型拓扑结构下，从节点不该有子节点
			if bitcaskNode.cf.Role == config.Slave && config.NodeTopology != config.Line {
				ticker.Stop()
				return
			}

			bitcaskNode.slavesInfo.Range(func(id, iInfo interface{}) bool {
				info, ok := iInfo.(slaveInfo)
				if !ok {
					log.Errorf("Convert iInfo[%v] to info failed", iInfo)
					return true
				}
				rpc := info.rpc
				alive, err := rpc.IsAlive(context.Background())
				if err != nil || !alive {
					log.Infof("MASTER : slave [%s] is not alive, remove.", id)
					bitcaskNode.RemoveSlave(id.(string))
				}
				return true
			})
		case <-ctx.Done():
			return
		}
	}
}
