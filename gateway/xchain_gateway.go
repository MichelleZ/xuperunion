/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package main

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	etcd3 "go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/resolver"

	"github.com/xuperchain/xuperunion/common/config"
	"github.com/xuperchain/xuperunion/common/log"
	ch "github.com/xuperchain/xuperunion/gateway/consistent_hash"
	re "github.com/xuperchain/xuperunion/gateway/etcd"
	loggw "github.com/xuperchain/xuperunion/gateway/log"
	"github.com/xuperchain/xuperunion/pb"
)

const (
	// MaxMsgSize max message size
	MaxMsgSize = 128 << 20
	// InitialWindowSize window size
	InitialWindowSize = 128 << 10
	// InitialConnWindowSize connetion window size
	InitialConnWindowSize = 64 << 10
	// ReadBufferSize buffer size
	ReadBufferSize = 32 << 10
	// WriteBufferSize write buffer size
	WriteBufferSize = 32 << 10
)

var (
	// EtcdConfig etcdClusterAddrs
	EtcdConfig = getEtcdConfig()
	// GwAddr gateway addr
	GwAddr = ":50089"
	// INVALID_PARAMETER_ERROR
	INVALID_PARAMETER_ERROR = errors.New("Parameters error")
)

type server struct {
	client pb.XchainClient
	xlog   log.Logger
}

func (s *server) PostTx(ctx context.Context, in *pb.TxStatus) (*pb.CommonReply, error) {
	s.xlog.Debug("PostTx start")

	if in.Tx == nil || in.Tx.TxInputs == nil {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	for _, v := range in.Tx.TxInputs {
		hashData = string(v.FromAddr)
		break
	}
	s.xlog.Info("PostTx Addr hashkey", "hashData", hashData)

	out, err := s.client.PostTx(context.WithValue(context.Background(), ch.HashKey, hashData), in, grpc.WaitForReady(true))
	if err != nil {
		return out, err
	}

	s.xlog.Debug("PostTx done")
	return out, nil
}

func (s *server) QueryACL(ctx context.Context, in *pb.AclStatus) (*pb.AclStatus, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.QueryACL(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (s *server) QueryTx(ctx context.Context, in *pb.TxStatus) (*pb.TxStatus, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.QueryTx(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (s *server) GetBalance(ctx context.Context, in *pb.AddressStatus) (*pb.AddressStatus, error) {
	s.xlog.Debug("GetBalance start")

	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	hashData = in.Address
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.GetBalance(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (s *server) GetFrozenBalance(ctx context.Context, in *pb.AddressStatus) (*pb.AddressStatus, error) {
	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	hashData = in.Address
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.GetFrozenBalance(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (s *server) GetBlock(ctx context.Context, in *pb.BlockID) (*pb.Block, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.GetBlock(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (s *server) GetBlockChainStatus(ctx context.Context, in *pb.BCStatus) (*pb.BCStatus, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.GetBlockChainStatus(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (s *server) GetBlockChains(ctx context.Context, in *pb.CommonIn) (*pb.BlockChains, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.GetBlockChains(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (s *server) GetSystemStatus(ctx context.Context, in *pb.CommonIn) (*pb.SystemsStatusReply, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.GetSystemStatus(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		return out, err
	}

	//out.SystemsStatus.PeerUrls = []string{}
	return out, nil
}

func (s *server) GetNetURL(ctx context.Context, in *pb.CommonIn) (*pb.RawUrl, error) {
	return nil, nil
}

func (s *server) SelectUTXO(ctx context.Context, in *pb.UtxoInput) (*pb.UtxoOutput, error) {
	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	hashData = in.Address
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.SelectUTXO(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain SelectUTXO failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) DeployNativeCode(ctx context.Context, in *pb.DeployNativeCodeRequest) (*pb.DeployNativeCodeResponse, error) {
	s.xlog.Info("Deploy nativacode")

	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	s.xlog.Info("in.Address", "deploy address", in.Address)
	hashData = in.Address

	out, err := s.client.DeployNativeCode(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain DeployNativeCode failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) NativeCodeStatus(ctx context.Context, in *pb.NativeCodeStatusRequest) (*pb.NativeCodeStatusResponse, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.NativeCodeStatus(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain NativeCodeStatus failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) DposCandidates(ctx context.Context, in *pb.DposCandidatesRequest) (*pb.DposCandidatesResponse, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.DposCandidates(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain DposCandidates failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) DposNominateRecords(ctx context.Context, in *pb.DposNominateRecordsRequest) (*pb.DposNominateRecordsResponse, error) {
	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	hashData = in.Address
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.DposNominateRecords(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Error("xchain DposNominateRecords failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) DposNomineeRecords(ctx context.Context, in *pb.DposNomineeRecordsRequest) (*pb.DposNomineeRecordsResponse, error) {
	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	hashData = in.Address
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.DposNomineeRecords(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain DposNomineeRecords failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) DposVoteRecords(ctx context.Context, in *pb.DposVoteRecordsRequest) (*pb.DposVoteRecordsResponse, error) {
	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	hashData = in.Address
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.DposVoteRecords(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain DposVoteRecords failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) DposVotedRecords(ctx context.Context, in *pb.DposVotedRecordsRequest) (*pb.DposVotedRecordsResponse, error) {
	if in.Address == "" {
		return nil, INVALID_PARAMETER_ERROR
	}

	var hashData string
	hashData = in.Address
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.DposVotedRecords(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain DposVotedRecords failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) DposCheckResults(ctx context.Context, in *pb.DposCheckResultsRequest) (*pb.DposCheckResultsResponse, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.DposCheckResults(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain DposCheckResults failed", "err", err)
		return out, err
	}

	return out, nil
}

func (s *server) PreExec(ctx context.Context, in *pb.InvokeRPCRequest) (*pb.InvokeRPCResponse, error) {
	var hashData string
	rand.Seed(time.Now().Unix())
	rnd := rand.Intn(10000)
	hashData = fmt.Sprintf("abcdefg%v", rnd)
	s.xlog.Info("Addr hashkey", "hashData", hashData)

	out, err := s.client.PreExec(context.WithValue(context.Background(), ch.HashKey, hashData), in)
	if err != nil {
		s.xlog.Warn("xchain PreExec failed", "err", err)
		return out, err
	}

	return out, nil
}

// getEtcdConfig get etcd config addr
func getEtcdConfig() etcd3.Config {
	flags := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	cfg := config.NewNodeConfig()
	cfg.LoadConfig(flags)
	cfg.ApplyFlags(flags)
	flags.StringVar(&GwAddr, "gwAddr", GwAddr, "used for config overwrite --gwAddr <addr path>")

	flags.Parse(os.Args[1:])

	etcdAddr := []string{}
	for _, addr := range strings.Split(cfg.EtcdClusterAddr, ",") {
		etcdAddr = append(etcdAddr, addr)
	}

	etcdConfig := etcd3.Config{
		Endpoints: etcdAddr,
	}

	return etcdConfig
}

func main() {
	xlog, err := log.OpenLog(loggw.CreateLog())
	if err != nil {
		fmt.Println(errors.New("OpenLog failed"))
	}

	xlog.Info("Start gateway")
	// GwAddr defalut: :50089
	// Modify it by cli tool --gwAddr x.x.x.x:1234
	lis, err := net.Listen("tcp", GwAddr)
	if err != nil {
		xlog.Error("failed to listen.", "err", err)
	}

	var rpcOptions []grpc.ServerOption
	rpcOptions = append(rpcOptions,
		grpc.MaxMsgSize(MaxMsgSize),
		grpc.ReadBufferSize(ReadBufferSize),
		grpc.InitialWindowSize(InitialWindowSize),
		grpc.InitialConnWindowSize(InitialConnWindowSize),
		grpc.WriteBufferSize(WriteBufferSize),
	)

	s := grpc.NewServer(rpcOptions...)

	r := re.NewResolver(EtcdConfig, xlog)
	resolver.Register(r)

	conn, err := grpc.Dial(r.Scheme()+":///gateway/xchaingateway", grpc.WithInsecure(), grpc.WithBalancerName(ch.Name))
	defer conn.Close()
	if err != nil {
		xlog.Error("did not connect", "err", err)
	}

	client := pb.NewXchainClient(conn)
	xlog.Info("initClient done")

	pb.RegisterXchainServer(s, &server{xlog: xlog, client: client})

	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		xlog.Error("failed to serve", "err", err)
	}
}
