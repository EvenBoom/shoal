# Shoal

## Introduction

Discovery services by etcd.

## Environment

golang 1.17 

## Basic Usage

- Download
```bash
go get -u github.com/EvenBoom/shoal
```

- Run Etcd

- Proto File
```proto
syntax = "proto3";

option go_package = "/hello";

package hello;

service Hello  {
  rpc Say (SayRequest) returns (SayResponse) {}
}

message SayRequest {
  string ask = 1;
}

message SayResponse {
  string answer = 1;
}
```

- Server
```golang
package main

import (
	"context"
	"log"
	pb "sample/proto/hello"

	"github.com/EvenBoom/shoal"
	"google.golang.org/grpc"
)

// HelloService hello service
type HelloService struct {
	pb.UnimplementedHelloServer
}

// Say say hello
func (s HelloService) Say(ctx context.Context, req *pb.SayRequest) (*pb.SayResponse, error) {
	resp := new(pb.SayResponse)
	resp.Answer = "answer to " + req.Ask
	return resp, nil
}

func main() {
	grpcServer := grpc.NewServer()
	pb.RegisterHelloServer(grpcServer, &HelloService{})

	helloSrv := shoal.NewEtcdService("hello")
	go helloSrv.Register()
	if err := grpcServer.Serve(helloSrv.Listener); err != nil {
		log.Fatal(err)
	}
}
```

- Client
```golang
package main

import (
	"context"
	"log"

	pb "sample/proto/hello"

	"github.com/EvenBoom/shoal"
	"google.golang.org/grpc"
)

func main() {

	service := shoal.NewEtcdClient()
	addr, err := service.GetSLBAddress("hello")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal(err)
	}

	cli := pb.NewHelloClient(conn)

	req := new(pb.SayRequest)
	req.Ask = "hello"

	rsp, err := cli.Say(context.Background(), req)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(rsp.Answer)
}
```
