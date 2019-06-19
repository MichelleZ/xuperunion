/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package etcd

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/xuperchain/log15"
	etcd3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"golang.org/x/net/context"
)

// ERegistry struct
type ERegistry struct {
	etcd3Client *etcd3.Client
	key         string
	value       string
	ttl         int64
	ctx         context.Context
	cancel      context.CancelFunc
	xlog        log.Logger
}

// Input NewRegistry parameters
type Input struct {
	// EtcdConfig etcd address
	EtcdConfig etcd3.Config
	// Service path but key in etcd need puls scheme
	// etcd key: scheme/Prefix eg. scheme=etcdxchain
	// Prefix=gateway/test
	// etcd key: etcdxchain/gateway/test
	Prefix string
	// GrpcAddr register address
	Addr string
	// TTL lease time
	TTL  int64
	Xlog log.Logger
}

// NewRegistry User entrance
func NewRegistry(input Input) (*ERegistry, error) {
	client, err := etcd3.New(input.EtcdConfig)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	registry := &ERegistry{
		etcd3Client: client,
		key:         "/" + scheme + "/" + input.Prefix + "/" + input.Addr,
		value:       input.Addr,
		ttl:         input.TTL,
		ctx:         ctx,
		cancel:      cancel,
		xlog:        input.Xlog,
	}

	registry.xlog.Debug("NewRegistry done", "register", registry)
	return registry, nil
}

// Register user's address is stored by etcd after user register
func (er *ERegistry) Register() error {
	putFunc := func() error {
		resp, _ := er.etcd3Client.Grant(er.ctx, int64(er.ttl))
		_, err := er.etcd3Client.Get(er.ctx, er.key)
		er.xlog.Debug("Register()", "key", er.key, "value", er.value)
		if err != nil {
			if err == rpctypes.ErrKeyNotFound {
				if _, err := er.etcd3Client.Put(er.ctx, er.key, er.value, etcd3.WithLease(resp.ID)); err != nil {
					er.xlog.Error("Gateway Register: set key connect to etcd3 failed.", "key", er.key, "err", err.Error())
				}
			} else {
				er.xlog.Error("Gateway Register: key connect to etcd3 failed.", "key", er.key, "err", err.Error())
			}
		} else {
			if _, err := er.etcd3Client.Put(er.ctx, er.key, er.value, etcd3.WithLease(resp.ID)); err != nil {
				er.xlog.Error("Gateway Register: refresh key connect to etcd3 failed", "key", er.key, "err", err.Error())
			}
		}

		return err
	}

	err := putFunc()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Second * time.Duration(er.ttl))
	defer ticker.Stop()
	er.xlog.Info("Register() start for select")
	for {
		select {
		case <-ticker.C:
			putFunc()
		case <-er.ctx.Done():
			ticker.Stop()
			if _, err := er.etcd3Client.Delete(context.Background(), er.key); err != nil {
				er.xlog.Error("Gateway ERegistry: UnRegister failed.", "key", er.key, "err", err.Error())
			}
			er.xlog.Info("Gateway ERegistry: UnRegister ok", "key", er.key)
			return nil
		}
	}

	return nil
}

// UnRegister Delete user'addr in etcd
func (er *ERegistry) UnRegister() error {
	er.cancel()
	time.Sleep(time.Second * 3)
	return nil
}

// getInfo for test
func (er *ERegistry) getInfo() {
	resp, err := er.etcd3Client.Get(context.Background(), er.key, etcd3.WithPrefix())
	if err != nil {
		er.xlog.Error("getInfo: key connect to etcd3 failed.", "key", er.key, "err", err.Error())
	}

	for i, val := range resp.Kvs {
		er.xlog.Info("resp kvs", "index", i, "key", string(val.Key), "val", string(val.Value))
	}
}

// GetServerIP get server addr
func GetServerIP() string {
	serverIPs := []string{}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				serverIPs = append(serverIPs, ipnet.IP.String())
			}
		}
	}

	return serverIPs[0]
}
