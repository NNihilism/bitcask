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
	fmt.Println("...1")
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
	fmt.Println("...2")

	// 调用对方的RegisterSlave方法
	rpcResp, err := c.RegisterSlave(context.Background(), &node.RegisterSlaveRequest{
		Address: bitcaskNode.cf.Addr,
		RunId:   bitcaskNode.cf.ID,
	})
	fmt.Println("...3")

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
	fmt.Println("...4")

	// 若成功，则改变自身状态信息
	if rpcResp.BaseResp.StatusCode == 0 {
		bitcaskNode.masterRpc = &c
		bitcaskNode.cf.Role = config.Slave
	}
	fmt.Println("...5")

	resp = new(node.SendSlaveofResponse)
	resp.BaseResp = rpcResp.BaseResp
	fmt.Println("...6")

	// 失败，则返回错误信息
	return
}
