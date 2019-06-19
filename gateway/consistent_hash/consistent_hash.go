/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package consistenthash

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/xuperchain/xuperunion/common/log"
	loggw "github.com/xuperchain/xuperunion/gateway/log"
)

const (
	// Name is the name of consistant_hash balancer
	Name = "consistant_hash"
	// HashKey hash key parameter
	HashKey = "hash_key"
)

// newBuilder creates a new consistant_hash balancer builder
func newBuilder() balancer.Builder {
	xlog, err := log.OpenLog(loggw.CreateLog())
	if err != nil {
		fmt.Println(errors.New("OpenLog failed"))
	}
	return base.NewBalancerBuilderWithConfig(Name, &chPickerBuilder{xlog: xlog}, base.Config{HealthCheck: true})
}

// init() register consistenthash balancer
func init() {
	balancer.Register(newBuilder())
}

// chPickerBuilder builder interface
type chPickerBuilder struct {
	xlog log.Logger
}

// Build realize base/base.go PickerBuilder interface
func (c *chPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	c.xlog.Info("consistenthashPicker: newPicker called with readySCs", "readySCs", readySCs)

	var addrs []string
	for raddr := range readySCs {
		addrs = append(addrs, raddr.Addr)
	}

	ketama := NewKetamaConsistentHash(10, c.xlog)
	for _, addr := range addrs {
		ketama.Add(addr)
	}

	return &chPicker{
		readySCs: readySCs,
		addrs:    addrs,
		ketama:   ketama,
		xlog:     c.xlog,
	}
}

// chPicker consistenthash struct
type chPicker struct {
	readySCs map[resolver.Address]balancer.SubConn
	addrs    []string
	ketama   *KetamaConsistentHash
	mu       sync.Mutex
	xlog     log.Logger
}

// Pick computed addr due to hashkey
func (ch *chPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.xlog.Info("Pick start")

	key, ok := ctx.Value(HashKey).(string)
	if ok {
		nodeAddr, ok := ch.ketama.Get(key)
		if ok {
			nodeAddr = ch.parseKey(nodeAddr)
			ch.xlog.Info("The distributed IP addr", "nodeAddr", nodeAddr)
			if sc, ok := ch.isExist(nodeAddr, ch.readySCs); ok {
				return sc, nil, nil
			}
		}
	}

	return nil, nil, balancer.ErrNoSubConnAvailable
}

// parseKey value by Get method contains replicas part, so remove it
func (ch *chPicker) parseKey(addr string) string {
	return strings.Split(addr, "-")[0]
}

// isExist make sure got add shoule be in readySCs
func (ch *chPicker) isExist(addr string, readySCs map[resolver.Address]balancer.SubConn) (balancer.SubConn, bool) {
	ch.xlog.Debug("chPicker isExist")
	for raddr := range readySCs {
		if addr == raddr.Addr {
			return readySCs[raddr], true
		}
	}

	return nil, false
}
