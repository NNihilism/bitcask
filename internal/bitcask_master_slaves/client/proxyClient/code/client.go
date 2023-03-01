package code

import "bitcaskDB/internal/bitcask_master_slaves/proxy/kitex_gen/prxyService/proxyservice"

// 供用户代码内嵌的模式

type BitcaskProxy struct {
	masterId  string
	rpcClient proxyservice.Client
}

func NewBitcaskProxy(masterId string) *BitcaskProxy {

	return &BitcaskProxy{
		masterId: masterId,
	}
}
