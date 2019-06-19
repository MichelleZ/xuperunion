/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/syndtr/goleveldb/leveldb/errors"
	etcd3 "go.etcd.io/etcd/clientv3"

	"github.com/xuperchain/xuperunion/common/config"
	"github.com/xuperchain/xuperunion/common/log"
	"github.com/xuperchain/xuperunion/core"
	re "github.com/xuperchain/xuperunion/gateway/etcd"
	"github.com/xuperchain/xuperunion/p2pv2"
	"github.com/xuperchain/xuperunion/server"
)

var (
	buildVersion = ""
	commitHash   = ""
	buildDate    = ""
)

// Start init and star chain node
func Start(cfg *config.NodeConfig) error {
	xlog, err := log.OpenLog(&cfg.Log)
	if err != nil {
		err := errors.New("open log fail")
		return err
	}
	xlog.Info("debug info", "root host", cfg.ConsoleConfig.Host)

	// start node
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	cfg.VisitAll()
	xlog.Trace("Hello BlockChain")

	// 注册优雅关停信号, 包括ctrl + C 和 kill 信号
	xlog.Trace("register stopping handler")
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigc)

	// Init p2pv2
	p2pV2Serv, err := p2pv2.NewP2PServerV2(cfg.P2pV2, xlog)
	if err != nil {
		panic(err)
	}

	xcmg := xchaincore.XChainMG{}
	if err = xcmg.Init(xlog, cfg, p2pV2Serv); err != nil {
		panic(err)
	}

	if cfg.CPUProfile != "" {
		perfFile, perr := os.Create(cfg.CPUProfile)
		if perr != nil {
			panic(perr)
		}
		pprof.StartCPUProfile(perfFile)
	}
	// 启动挖矿结点
	xcmg.Start()
	go server.SerRun(&xcmg)

	registry := &re.ERegistry{}
	if cfg.GatewaySwitch {
		registry, err = initRegistry(cfg, xlog)
		if err != nil {
			xlog.Error("New Register failed", "err", err)
		}
		// 服务注册
		go registry.Register()
	}

	for {
		select {
		case <-sigc:
			xlog.Info("Got terminate, start to shutting down, please wait...")
			close(xcmg.Quit)
		case <-xcmg.Quit:
			xlog.Info("Got xcmg quit, start to shutting down, please wait...")
			Stop(&xcmg)

			if cfg.GatewaySwitch {
				xlog.Info("Unregister start")
				registry.UnRegister()
				xlog.Info("Unregister done")
			}

			return nil
		}
	}
}

// Stop gracefully shut down, 各个模块实现自己需要优雅关闭的资源并在此处调用即可
func Stop(xchainmg *xchaincore.XChainMG) {
	if xchainmg.Cfg.CPUProfile != "" {
		pprof.StopCPUProfile()
	}
	if xchainmg.Cfg.MemProfile != "" {
		f, err := os.Create(xchainmg.Cfg.MemProfile)
		if err == nil {
			pprof.WriteHeapProfile(f)
			f.Close()
		}
	}
	xchainmg.Stop()
	xchainmg.Log.Info("All modules have stopped!")
	pprof.StopCPUProfile()
	return
}

func initRegistry(cfg *config.NodeConfig, xlog log.Logger) (*re.ERegistry, error) {
	port := strings.TrimPrefix(cfg.TCPServer.Port, ":")
	ipAddr := re.GetServerIP()
	xlog.Debug("Server addr", "ip", ipAddr, "port", port)
	etcdAddr := []string{}
	for _, addr := range strings.Split(cfg.EtcdClusterAddr, ",") {
		etcdAddr = append(etcdAddr, addr)
	}
	etcdConfig := etcd3.Config{
		Endpoints: etcdAddr,
	}
	registry, err := re.NewRegistry(
		re.Input{
			EtcdConfig: etcdConfig,
			Prefix:     "gateway/xchaingateway",
			Addr:       fmt.Sprintf("%v:%v", ipAddr, port),
			TTL:        5,
			Xlog:       xlog,
		})
	if err != nil {
		return nil, err
	}
	return registry, nil
}

func main() {
	flags := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	var showVersion bool
	flags.BoolVar(&showVersion, "version", false, "show xchain version")

	cfg := config.NewNodeConfig()
	cfg.LoadConfig(flags)
	cfg.ApplyFlags(flags)

	flags.Parse(os.Args[1:])

	if showVersion {
		fmt.Printf("%s-%s %s\n", buildVersion, commitHash, buildDate)
		return
	}

	err := Start(cfg)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(-1)
	}
}
