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
	fmt.Println("...4")

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
	fmt.Println("...5")

	if _, ok := bitcaskNode.slavesRpc[req.RunId]; ok {
		return &node.RegisterSlaveResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_SlaveofErrCode),
				StatusMessage: fmt.Sprintf("Already a slave node of node [ip:% s, runid:% s].", bitcaskNode.cf.Addr, bitcaskNode.cf.ID),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}
	fmt.Println("...6")

	// 添加slave
	// 1. rpc初始化
	if len(bitcaskNode.slavesRpc) == 0 {
		bitcaskNode.slavesRpc = make(map[string]nodeservice.Client)
	}
	fmt.Println("...7")

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
	fmt.Println("...8")

	bitcaskNode.slavesRpc[req.RunId] = c
	fmt.Println("...9")

	// 2. 修改变量
	bitcaskNode.cf.ConnectedSlaves += 1
	// 返回结果
	return &node.RegisterSlaveResponse{
		BaseResp: &node.BaseResp{
			StatusCode:    int64(node.ErrCode_SuccessCode),
			StatusMessage: fmt.Sprintf("Slaveof node [ip:% s, runid:% s] successfully.", bitcaskNode.cf.Addr, bitcaskNode.cf.ID),
			ServiceTime:   time.Now().Unix(),
		},
	}, nil
}
