package shoal

import (
	"fmt"
	"strconv"
	"testing"
)

func TestEtcdPut(t *testing.T) {

	// EtcdPut()
	srv := NewEtcdService("test")
	err := srv.Register()
	if err != nil {
		fmt.Println(err)
	}
	// var i int32
	// atomic.AddInt32(&i, 1)

	// 优雅退出
	// c := make(chan os.Signal, 10)
	// // 监听信号
	// signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, os.Interrupt)

	// time.Sleep(5 * time.Second)
	// fmt.Println("退出中")
	// srv.Unregister()
	// fmt.Println("退出")
}

func TestGetSLBAddress(t *testing.T) {

	// EtcdPut()
	srv := NewEtcdService("test")
	srv.SetAddress("127.0.0.1:111")

	// srv.SetLocalCache(false)
	srv.SetSLBScore(5)

	go srv.Register()

	// time.Sleep(time.Second)

	for {
		addr, _ := srv.GetSLBAddress("test")
		fmt.Println("slb address: " + addr)
		for name, servers := range srv.ScoreSLB.ScoreServers {
			fmt.Println("cache name: " + name)
			for _, server := range servers {
				fmt.Println("cache servers: " + server.Address + "    cache score: " + strconv.FormatInt(server.Score, 10) + "    cache slb score: " + strconv.FormatInt(server.SLBScore, 10) + "    cache version: " + strconv.FormatInt(server.Version, 10))
			}
		}
	}
}
