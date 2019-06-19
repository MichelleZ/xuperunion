/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package etcd

import (
	"fmt"

	"github.com/xuperchain/log15"
	etcd3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc/resolver"
)

const scheme = "etcdxchain"

//EResolver realize resolver.Builder interface
type EResolver struct {
	Config etcd3.Config
	cc     resolver.ClientConn
	client *etcd3.Client
	xlog   log.Logger
}

// NewResolver user entrance
func NewResolver(cfg etcd3.Config, xlog log.Logger) resolver.Builder {
	return &EResolver{Config: cfg, xlog: xlog}
}

// Build creates a new resolver for the given target
func (ere *EResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	ere.xlog.Info("EResolve Build start")
	client, err := etcd3.New(ere.Config)
	if err != nil {
		return nil, fmt.Errorf("Create etcd3 client error: %v", err)
	}
	ere.client = client

	ere.cc = cc

	ere.xlog.Info("watch start")
	go ere.watch("/" + target.Scheme + "/" + target.Endpoint + "/")

	return ere, nil
}

// watch
func (ere *EResolver) watch(key string) {
	var addrs []resolver.Address

	resp, err := ere.client.Get(context.Background(), key, etcd3.WithPrefix())
	if err == nil {
		addrs = ere.getAddrs(resp)
		for i := range addrs {
			ere.xlog.Info("watch addrs", "addr", addrs[i].Addr)
		}
	} else {
		ere.xlog.Warn("EResolover Watche Get key error", "err", err)
	}

	ere.cc.NewAddress(addrs)

	ew := ere.client.Watch(context.Background(), key, etcd3.WithPrefix())
	for wresp := range ew {
		for _, ev := range wresp.Events {
			addr := string(ev.Kv.Value)
			switch ev.Type {
			case mvccpb.PUT:
				if !ere.isExist(addrs, addr) {
					addrs = append(addrs, resolver.Address{Addr: addr})
					ere.cc.NewAddress(addrs)
				}
			case mvccpb.DELETE:
				if newAddrs, ok := ere.remove(addrs, addr); ok {
					addrs = newAddrs
					ere.cc.NewAddress(addrs)
				}
			}

			//ere.xlog.Debug("for watch", "ev.Type", ev.Type, "ev.Kv.Key", ev.Kv.Key, "ev.Kv.Value", ev.Kv.Value)
		}
	}
}

// getAddrs Get addrs from etcd3.GetResponse
func (ere *EResolver) getAddrs(resp *etcd3.GetResponse) []resolver.Address {
	addrs := []resolver.Address{}

	if resp == nil || resp.Kvs == nil {
		return addrs
	}

	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			addrs = append(addrs, resolver.Address{Addr: string(v)})
		}
	}
	return addrs
}

// isExist addr in addrs or not
func (ere *EResolver) isExist(addrs []resolver.Address, addr string) bool {
	for _, a := range addrs {
		if a.Addr == addr {
			return true
		}
	}

	return false
}

func (ere *EResolver) remove(addrs []resolver.Address, addr string) ([]resolver.Address, bool) {
	for i := range addrs {
		if addrs[i].Addr == addr {
			addrs[i] = addrs[len(addrs)-1]
			return addrs[:len(addrs)-1], true
		}
	}

	return nil, false
}

// Scheme returns the scheme supported by this resolver
func (ere *EResolver) Scheme() string {
	return scheme
}

// ResolveNow will be called by gRPC to try to resolve the target name
// again. It's just a hint, resolver can ignore this if it's not necessary
// resolver.Resolver interface need to realize ResolveNow() and Close()
func (ere *EResolver) ResolveNow(rn resolver.ResolveNowOption) {
	ere.xlog.Info("EResolver.ResolveNow")
}

// Close closes the resolver
func (ere *EResolver) Close() {
	ere.xlog.Info("EResolver.Close")
}
