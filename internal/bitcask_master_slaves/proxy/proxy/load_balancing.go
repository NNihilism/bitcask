package proxy

import (
	"time"
)

var currentWeight, effectiveWeight []int
var totalWeight int
var lastTime int64

func (proxy *Proxy) selectNode() string {
	// 选择一个合适的节点进行请求转发
	proxy.mu.RLock()
	defer proxy.mu.RUnlock()

	if proxy.lastNodeUpdate > lastTime {
		proxy.resetLoadBalancing()
	}

	return proxy.updateLoadBalancing()

}

func (proxy *Proxy) resetLoadBalancing() {
	currentWeight = make([]int, len(proxy.node))
	effectiveWeight = make([]int, len(proxy.node))
	totalWeight = 0
	for i, info := range proxy.node {
		effectiveWeight[i] = info.weight
		totalWeight += info.weight
	}

	lastTime = time.Now().Unix()
}

func (proxy *Proxy) updateLoadBalancing() string {
	res_idx := 0
	for i := range effectiveWeight {
		currentWeight[i] += effectiveWeight[i]
		if currentWeight[i] > currentWeight[res_idx] {
			res_idx = i
		}
	}
	currentWeight[res_idx] -= totalWeight
	return proxy.node[res_idx].id
}
