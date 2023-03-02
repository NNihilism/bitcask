package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/log"
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/kitex/client"
)

func (bitcaskNode *BitcaskNode) SendSlaveOfReq(req *node.SendSlaveofRequest) (resp *node.SendSlaveofResponse, err error) {
	// 初始化临时rpc客户端
	c, err := nodeservice.NewClient(
		consts.NodeServiceName,
		client.WithHostPorts(req.Address),
	)
	if err != nil {
		log.Errorf("Init rpcclient err [%v]", err)
		return &node.SendSlaveofResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_SlaveofErrCode),
				StatusMessage: fmt.Sprintf("Init rpcclient err [%v]", err),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	// 调用对方的RegisterSlave方法
	rpcResp, err := c.RegisterSlave(context.Background(), &node.RegisterSlaveRequest{
		Address: bitcaskNode.cf.Addr,
		RunId:   bitcaskNode.cf.ID,
		Weight:  int32(bitcaskNode.cf.Weight),
	})

	if err != nil {
		// log.Errorf("Init rpcclient err [%v]", err)
		return &node.SendSlaveofResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_SlaveofErrCode),
				StatusMessage: fmt.Sprintf("RegisterSlave err [%v]", err),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	// 若成功，则改变自身状态信息
	if rpcResp.BaseResp.StatusCode == 0 {
		log.Infof("Connecting to MASTER %s", rpcResp.RunId)
		bitcaskNode.masterRpc = c
		bitcaskNode.cf.Role = config.Slave
		bitcaskNode.cf.MasterId = rpcResp.RunId
		bitcaskNode.resetReplication(true)
	}

	resp = new(node.SendSlaveofResponse)
	resp.BaseResp = rpcResp.BaseResp

	// 请求数据同步
	go bitcaskNode.sendPSyncReq()

	return
}

// 从节点给主节点发送PsyncReq请求
func (bitcaskNode *BitcaskNode) sendPSyncReq() {
	log.Infof("MASTER <-> REPLICA sync started")

	resp, err := bitcaskNode.masterRpc.PSyncReq(context.Background(), &node.PSyncRequest{
		MasterId: bitcaskNode.cf.MasterId,
		SlaveId:  bitcaskNode.cf.ID,
		Offset:   int64(bitcaskNode.cf.CurReplicationOffset), // 申请已有的下一个
	})
	if err != nil {
		return
	}

	if resp.Code == int8(config.Fail) {
		return
	}

	if resp.Code != int8(config.IncreReplSync) {
		log.Infof("Partial resynchronization not possible (no cached master)")
	}

	if resp.Code == int8(config.FullReplSync) {
		bitcaskNode.syncStatus = nodeInFullRepl // 只有全量复制时才开启这个变量，开启后对于写请求不会对序列号进行判断，而是直接写入
		bitcaskNode.resetReplication(true)
		log.Infof("Full resync from master: [%s]", bitcaskNode.cf.MasterId)
		bitcaskNode.sendPSyncReady() // 通知master可以开始发送数据了
	}
}

// 从节点给主节点发送PsyncReady请求
// 只有全量复制时需要发送这个请求,增量复制不需要
func (bitcaskNode *BitcaskNode) sendPSyncReady() {
	req := &node.PSyncRequest{
		MasterId: bitcaskNode.cf.MasterId,
		SlaveId:  bitcaskNode.cf.ID,
		Offset:   int64(bitcaskNode.cf.CurReplicationOffset),
	}

	resp, err := bitcaskNode.masterRpc.PSyncReady(context.Background(), req)
	if resp.Code != int8(config.FullReplSync) {
		log.Errorf("full replication err [%v]", err)
	}
}

// Slave接收到复制结束通知
func (bitcaskNode *BitcaskNode) HandleReplFinishNotify(req *node.ReplFinishNotifyReq) (bool, error) {
	if req.Ok {
		bitcaskNode.syncStatus = nodeInIdle

		if req.SyncType == int8(config.FullReplSync) {
			// 如果是全量复制结束 则需要校验下最后收到的id是否正确,如果正确,则修改offset
			if req.LastEntryId != int64(bitcaskNode.cf.CurReplicationOffset) {
				log.Errorf("full replication error. 'LastEntryId' does not match.  LastEntryId : [%d], CurReplicationOffset : [%d]", req.LastEntryId, bitcaskNode.cf.CurReplicationOffset)
				return false, nil
			}
			bitcaskNode.cf.CurReplicationOffset = int(req.MasterOffset)
			bitcaskNode.cf.MasterReplicationOffset = int(req.MasterOffset)
			log.Infof("MASTER <-> REPLICA sync: Finished with success")
		}
		return true, nil
	}
	return true, nil
}
