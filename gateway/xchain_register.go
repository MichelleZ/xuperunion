/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	//"time"

	"github.com/spf13/pflag"
	etcd3 "go.etcd.io/etcd/clientv3"

	"github.com/xuperchain/xuperunion/common/config"
	"github.com/xuperchain/xuperunion/common/log"
	registry "github.com/xuperchain/xuperunion/gateway/etcd"
	loggw "github.com/xuperchain/xuperunion/gateway/log"
)

// getPortAndEtcdConfig get port from config TCP port file
// get etcdConfig from EtcdClusterAddr
func getPortAndEtcdConfig() (string, etcd3.Config) {
	flags := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	cfg := config.NewNodeConfig()
	cfg.LoadConfig(flags)
	cfg.ApplyFlags(flags)
	flags.Parse(os.Args[1:])

	port := strings.TrimPrefix(cfg.TCPServer.Port, ":")

	etcdAddr := []string{}
	for _, addr := range strings.Split(cfg.EtcdClusterAddr, ",") {
		etcdAddr = append(etcdAddr, addr)
	}

	etcdConfig := etcd3.Config{
		Endpoints: etcdAddr,
	}

	return port, etcdConfig
}

func main() {
	xlog, err := log.OpenLog(loggw.CreateLog())
	if err != nil {
		fmt.Println(errors.New("OpenLog failed"))
	}

	ipAddr := registry.GetServerIP()
	port, etcdConfig := getPortAndEtcdConfig()
	xlog.Debug("Server addr", "ip", ipAddr, "port", port)

	registry, err := registry.NewRegistry(
		registry.Input{
			EtcdConfig: etcdConfig,
			Prefix:     "gateway/xchaingateway",
			Addr:       fmt.Sprintf("%v:%v", ipAddr, port),
			TTL:        5,
			Xlog:       xlog,
		})

	if err != nil {
		xlog.Error("New Register failed", "err", err)
		return
	}

	// 服务注册
	go registry.Register()

	// 优雅信号退出
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT, os.Interrupt)
	defer signal.Stop(sigc)

	for {
		select {
		case <-sigc:
			xlog.Info("Unregister start")
			registry.UnRegister()
			xlog.Info("Unregister done")
			return
		}
	}
}
