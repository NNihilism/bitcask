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
		bitcaskNode.masterRpc = c
		bitcaskNode.cf.Role = config.Slave
		bitcaskNode.cf.MasterId = rpcResp.RunId
	}

	resp = new(node.SendSlaveofResponse)
	resp.BaseResp = rpcResp.BaseResp

	// 请求数据同步
	go bitcaskNode.sendPSyncReq()

	return
}

// 从节点给主节点发送Psync请求
func (bitcaskNode *BitcaskNode) sendPSyncReq() {
	// 是不是该判断下状态，避免重复发送请求？ 或者在master判断，拒绝给同一个slave创建多个协程进行增量/全量更新
	resp, err := bitcaskNode.masterRpc.PSync(context.Background(), &node.PSyncRequest{
		MasterId: bitcaskNode.cf.MasterId,
		SlaveId:  bitcaskNode.cf.ID,
		Offset:   int64(bitcaskNode.cf.CurReplicationOffset), // 申请已有的下一个
	})
	if err != nil {
		return
	}
	if resp.Code == int8(config.FullReplSync) {
		bitcaskNode.syncStatus = nodeInFullRepl // 只有全量复制时才开启这个变量，开启后对于写请求不会对序列号进行判断，而是直接写入
	}
}

// Slave增量复制失败
func (bitcaskNode *BitcaskNode) HandleRepFailNotify(masterId string) (bool, error) {
	// 请求全量复制
	// 改变状态
	bitcaskNode.syncStatus = nodeInFullRepl
	// 发送请求
	resp, err := bitcaskNode.masterRpc.PSync(context.Background(), &node.PSyncRequest{
		MasterId: bitcaskNode.cf.MasterId,
		Offset:   -1,
		SlaveId:  bitcaskNode.cf.ID,
	})
	if err != nil {
		return false, err
	}
	if resp.Code != int8(config.FullReplSync) {
		return false, err
	}
	bitcaskNode.cf.CurReplicationOffset = 0 // 进度清0
	return true, nil
}

// Slave接收到复制结束通知
func (bitcaskNode *BitcaskNode) HandleReplFinishNotify(req *node.ReplFinishNotifyReq) (bool, error) {
	if req.Ok {
		bitcaskNode.syncStatus = nodeInIdle
		return true, nil
	}
	return true, nil
}
