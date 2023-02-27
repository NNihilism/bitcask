package client

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/util"
	"context"
)

func opLogEntry(client *Client, cmd []byte, args [][]byte) (interface{}, error) {
	resp, err := client.rpcClient.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   string(cmd),
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if result, err := ToString(resp); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}
