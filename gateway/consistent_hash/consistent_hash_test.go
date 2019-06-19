/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package consistenthash

import (
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	pb "github.com/xuperchain/xuperunion/gateway/consistent_hash/helloworld"
)

type testServer struct {
	pb.GreeterServer
}

func (s *testServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

type test struct {
	servers   []*grpc.Server
	addresses []string
}

func (t *test) cleanup() {
	for _, s := range t.servers {
		s.Stop()
	}
}

func startTestServers(count int) (_ *test, err error) {
	t := &test{}

	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()

	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:5005%d", i))
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		pb.RegisterGreeterServer(s, &testServer{})
		t.servers = append(t.servers, s)
		t.addresses = append(t.addresses, lis.Addr().String())

		go func(s *grpc.Server, l net.Listener) {
			s.Serve(l)
		}(s, lis)
	}

	return t, nil
}

func TestSimple(t *testing.T) {
	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v.", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(test.addresses[0], grpc.WithInsecure())
	if err != nil {
		t.Fatalf("failed to dial: %v.", err)
	}
	defer cc.Close()

	testc := pb.NewGreeterClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := testc.SayHello(ctx, &pb.HelloRequest{Name: "world"})
	if err != nil {
		t.Fatalf("SayHello() err: %v.", err)
	}

	fmt.Println("res:", res.Message)
}

func TestOneBackendConsistentHash(t *testing.T) {
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v.", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(Name))
	if err != nil {
		t.Fatalf("failed to dial: %v.", err)
	}
	defer cc.Close()

	testc := pb.NewGreeterClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for _, addr := range test.addresses {
		fmt.Println("addr: ", addr)
	}

	r.NewAddress([]resolver.Address{{Addr: test.addresses[0]}})
	if _, err := testc.SayHello(context.WithValue(ctx, HashKey, "hello"), &pb.HelloRequest{Name: "world"}); err != nil {
		t.Fatalf("SayHello() err: %v.", err)
	}
}

func TestBackendsConsistentHash(t *testing.T) {
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	backendCount := 5
	test, err := startTestServers(backendCount)
	if err != nil {
		t.Fatalf("failed to start servers: %v.", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(Name))
	if err != nil {
		t.Fatalf("failed to dial: %v.", err)
	}
	defer cc.Close()

	testc := pb.NewGreeterClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var resolvedAddrs []resolver.Address
	for i := 0; i < backendCount; i++ {
		resolvedAddrs = append(resolvedAddrs, resolver.Address{Addr: test.addresses[i]})
	}

	r.NewAddress(resolvedAddrs)

	var p peer.Peer
	rand.Seed(time.Now().Unix())
	for i := 0; i < 3*backendCount; i++ {
		rnd := rand.Intn(10000)
		hashData := fmt.Sprintf("aaaa 10 %v ", rnd)

		if _, err := testc.SayHello(context.WithValue(ctx, HashKey, hashData), &pb.HelloRequest{Name: "world"}, grpc.Peer(&p)); err != nil {
			t.Fatalf("SayHello() err: %v.", err)
		}

		t.Logf("Index %d: peer: %v", i, p.Addr.String())
	}
}
