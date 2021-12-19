package shoal

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/EvenBoom/shoal/slb"
	"google.golang.org/grpc/connectivity"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

/*
what is etcd? It is "A distributed, reliable key-value store for the most critical data of a distributed system".
This is copy from official website.
This file contains functions for etcd system.
*/

// errors.
var (
	ErrNoAvailableConfig  = errors.New("register: no available configuration")
	ErrNoAvailableName    = errors.New("register: no available name")
	ErrWrongNameFormat    = errors.New("register: wrong name format")
	ErrNoAvailableAddress = errors.New("register: no available address")
	ErrKeepAliveFailed    = errors.New("register: keep alive failed")
	ErrServiceNotActive   = errors.New("register: service not active")
)

// file path.
var (
	EtcdConfigPath = "config/etcd_clientv3.json"
)

// StatusType etcd service type.
type StatusType string

const (
	StatusTypeCreated StatusType = "CREATED"
	StatusTypeActive  StatusType = "ACTIVE"
	StatusTypeClosing StatusType = "CLOSING"
	StatusTypeClosed  StatusType = "CLOSED"
)

// default value.
var (
	DefaultAddress = "127.0.0.1:50051"
)

// EtcdService micro service base on etcd.
type EtcdService struct {
	Context        context.Context    // service context.
	Config         *clientv3.Config   // config of etcd clientv3.
	Client         *clientv3.Client   // client etcd clientv3.
	Name           string             // service name, "_" is keyword, you can't use it.
	Address        string             // service address.
	LeaseID        clientv3.LeaseID   // etcd lease id.
	LeaseTTL       int64              // etcd lease ttl(second) default 10s.
	SLBScore       int64              // score of server load balancing, must be larger than 0.
	WatchCancel    context.CancelFunc // cancel func of etcd watching.
	SLBWatchCancel context.CancelFunc // cancel func of slb.
	Logger         *zap.Logger        // zap logger.
	*slb.ScoreSLB                     // score server load balancing.
	Status         StatusType         // the status of service.
	sync.RWMutex                      // read-write mutex.
}

const deadline = time.Second

// configFile etcd config files.
//go:embed config/etcd_*.json
var configFile embed.FS

// NewEtcdService create etcd service.
func NewEtcdService(name string) *EtcdService {

	// init a etcd service.
	service := new(EtcdService)
	// init context.
	service.Context = context.Background()

	// name can not contain "_".
	if strings.Contains(service.Name, "_") {
		panic(ErrWrongNameFormat.Error())
	}

	service.Logger, _ = zap.NewProduction()

	// must be lower case letters.
	service.Name = strings.ToLower(name)
	// lease ttl default value 10s.
	service.LeaseTTL = 10
	// set default address.
	service.Address = DefaultAddress
	// init score slb.
	service.ScoreSLB = slb.NewScoreSLB()
	// local cache default value is true.
	service.ScoreSLB.LocalCache = true
	// set SLBScore default value 1.
	service.SLBScore = 1
	// set service status ready
	service.Status = StatusTypeCreated

	return service
}

// NewEtcdClient create etcd service with client, but this service has no local cache.
func NewEtcdClient() *EtcdService {

	service := new(EtcdService)
	// init context.
	service.Context = context.Background()

	service.Logger, _ = zap.NewProduction()
	// init score slb.
	service.ScoreSLB = slb.NewScoreSLB()
	// set default etcd config.
	err := service.setDefaultEtcdConfig()
	if err != nil {
		service.Logger.Panic(err.Error())
	}

	return service
}

// SetConfig set etcd clientv3 config.
func (srv *EtcdService) SetConfig(config *clientv3.Config) *EtcdService {
	srv.Config = config
	return srv
}

// SetConfig set etcd clientv3 config.
func (srv *EtcdService) SetAddress(address string) *EtcdService {
	srv.Address = address
	return srv
}

// SetEndPoints set end points.
func (srv *EtcdService) SetEndPoints(endPoints []string) *EtcdService {
	if srv.Config == nil {
		// if config is nil, init one.
		srv.SetConfig(new(clientv3.Config))
	}

	srv.Config.Endpoints = endPoints
	return srv
}

// SetLeaseTTL set lease ttl.
// if ttl smaller or equal than 0, will set default value.
func (srv *EtcdService) SetLeaseTTL(ttl int64) *EtcdService {
	srv.LeaseTTL = ttl
	if srv.LeaseTTL <= 0 {
		srv.LeaseTTL = 10
	}

	return srv
}

// SetSLBScore set score of server load balancing.
// if score smaller than 1, will set 1.
func (srv *EtcdService) SetSLBScore(score int64) *EtcdService {
	srv.SLBScore = score
	if srv.SLBScore < 1 {
		srv.SLBScore = 1
	}

	return srv
}

// AddEndPoint add a end point.
func (srv *EtcdService) AddEndPoint(endPoint string) *EtcdService {
	if srv.Config == nil {
		// if config is nil, init one.
		srv.SetConfig(new(clientv3.Config))
	}

	srv.Config.Endpoints = append(srv.Config.Endpoints, endPoint)
	return srv
}

// SetLogger set logger.
func (srv *EtcdService) SetLogger(logger *zap.Logger) *EtcdService {
	if logger == nil {
		return srv
	}

	srv.Logger = logger
	return srv
}

// NewZapLogger new logger.
func NewLogger() *zap.Logger {

	logger, _ := zap.NewProduction()

	return logger
}

// NewZapLoggerConfig new logger config.
func NewLoggerConfig() zap.Config {
	return zap.NewProductionConfig()
}

// InitClient init etcd client.
func (srv *EtcdService) InitClient() (*EtcdService, error) {

	if srv.Client == nil {

		if srv.Config == nil {
			srv.Logger.Error(ErrNoAvailableConfig.Error())
			return srv, ErrNoAvailableConfig
		}

		cli, err := clientv3.New(*srv.Config)
		if err != nil {
			srv.Logger.Error(err.Error())
			return srv, err
		}

		srv.Client = cli
	}

	return srv, nil
}

// Register register micro service.
func (srv *EtcdService) Register() error {

	if srv.Name == "" {
		srv.Logger.Error(ErrNoAvailableName.Error())
		return ErrNoAvailableName
	}

	if strings.Contains(srv.Name, "_") {
		srv.Logger.Error(ErrWrongNameFormat.Error())
		return ErrWrongNameFormat
	}

	if srv.Address == "" {
		srv.Logger.Error(ErrNoAvailableAddress.Error())
		return ErrNoAvailableAddress
	}

	// if config is nil, set default config.
	if srv.Config == nil {
		err := srv.setDefaultEtcdConfig()
		if err != nil {
			srv.Logger.Error(err.Error())
			return err
		}
	}

	for {
		srv.RLock()
		if srv.Status != StatusTypeActive {
			if srv.Status == StatusTypeClosing {
				srv.Status = StatusTypeClosed
			}
			srv.RUnlock()
			srv.Logger.Error(ErrServiceNotActive.Error(),
				zap.String("srv.Name", srv.Name),
			)
			return ErrServiceNotActive
		}
		srv.RUnlock()

		srv.keepAlive()
		time.After(time.Second)
	}

}

// setDefaultEtcdConfig set the default etcd config.
func (srv *EtcdService) setDefaultEtcdConfig() error {
	// etcd config file bytes.
	var configBytes []byte
	// config object of etcd clientv3.
	var config *clientv3.Config

	configBytes, err := configFile.ReadFile(EtcdConfigPath)
	if err != nil {
		return err
	}

	// set default config from file.
	config = new(clientv3.Config)
	err = json.Unmarshal(configBytes, config)
	if err != nil {
		return err
	}

	srv.SetConfig(config).InitClient()
	srv.Status = StatusTypeActive

	return nil
}

// keepAlive keep etcd service alive.
func (srv *EtcdService) keepAlive() {

	if srv.Client.ActiveConnection().GetState() == connectivity.TransientFailure {
		srv.Client.ActiveConnection().ResetConnectBackoff()
	}

	timeoutContext, cancel := context.WithTimeout(srv.Context, deadline)
	defer cancel()

	withPrefixOption := clientv3.WithPrefix()
	withPrevKVOption := clientv3.WithPrevKV()

	deleteKey := srv.Name + "_" + srv.Address + "_"

	// delete old key.
	deleteResp, err := srv.Client.Delete(timeoutContext, deleteKey, withPrefixOption, withPrevKVOption)
	if err != nil {
		srv.Logger.Error(err.Error())
		return
	}

	for _, kv := range deleteResp.PrevKvs {
		srv.Logger.Info("register: etcd delete key",
			zap.ByteString("kv.Key", kv.Key),
			zap.ByteString("kv.Value", kv.Value),
			zap.Int64("deleteResp.Header.Revision", deleteResp.Header.Revision),
		)
	}

	// lease grant.
	grantResp, err := srv.Client.Grant(timeoutContext, srv.LeaseTTL)
	if err != nil {
		srv.Logger.Error(err.Error())
		return
	}

	oldLeaseID := strconv.FormatInt(int64(srv.LeaseID), 16)

	if srv.LeaseID != 0 {

		// delete old lease id, this happens when etcd is unreachable.
		_, err = srv.Client.Revoke(timeoutContext, srv.LeaseID)
		if err == rpctypes.ErrLeaseNotFound {
			srv.Logger.Warn("register: "+err.Error(),
				zap.String("oldLeaseID", oldLeaseID),
			)
		} else if err != nil {
			srv.Logger.Error(err.Error())
			return
		}

	}

	srv.LeaseID = grantResp.ID

	newLeaseID := strconv.FormatInt(int64(srv.LeaseID), 16)

	putKey := srv.Name + "_" + srv.Address + "_" + newLeaseID
	putValue := strconv.FormatInt(srv.SLBScore, 10)

	withLeaseOption := clientv3.WithLease(srv.LeaseID)

	_, err = srv.Client.Put(timeoutContext, putKey, putValue, withLeaseOption)
	if err != nil {
		srv.Logger.Error(err.Error())
		return
	}

	srv.Logger.Info("register: register etcd service",
		zap.String("srv.Name", srv.Name),
		zap.String("newLeaseID", newLeaseID),
		zap.String("srv.Address", srv.Address),
	)

	cancelContext, cancel := context.WithCancel(srv.Context)
	defer cancel()

	srv.WatchCancel = cancel

	watchChan := srv.Client.Watch(cancelContext, putKey)

	keepAliveChan, err := srv.Client.KeepAlive(cancelContext, srv.LeaseID)
	if err != nil {
		srv.Logger.Error(err.Error())
		return
	}

	for {
		select {
		case watchResp := <-watchChan:
			if watchResp.Canceled {
				srv.Logger.Error(ErrServiceNotActive.Error(),
					zap.String("srv.Name", srv.Name),
				)
				return
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					srv.Logger.Info("register: delete key",
						zap.ByteString("event.Kv.Key", event.Kv.Key),
						zap.ByteString("event.Kv.Value", event.Kv.Value),
						zap.Int64("watchResp.Header.Revision", watchResp.Header.Revision),
					)
					return
				}
			}
		case keepAliveResp := <-keepAliveChan:
			if keepAliveResp == nil {
				srv.Logger.Error(ErrKeepAliveFailed.Error())
				return
			} else {
				srv.Logger.Info("register: keep alive success",
					zap.String("newLeaseID", newLeaseID),
				)
			}
		}

	}
}

// GetSLBAddress get address after server load balancing.
func (srv *EtcdService) GetSLBAddress(name string) (address string, err error) {

	srv.RLock()
	defer srv.RUnlock()

	if srv.Status != StatusTypeActive {
		return srv.getSLBAddressWithoutCache(name)
	}

	var scoreServers []*slb.ScoreServer

	if srv.LocalCache {

		address, err = srv.ScoreSLB.GetSLBAddress(name)
		if err == slb.ErrNoScoreServers {

			timeoutContext, cancel := context.WithTimeout(srv.Context, deadline)
			defer cancel()

			withPrefixOption := clientv3.WithPrefix()

			getKey := name + "_"

			getResp, err := srv.Client.Get(timeoutContext, getKey, withPrefixOption)
			if err != nil {
				return "", err
			}

			scoreServers = make([]*slb.ScoreServer, len(getResp.Kvs))

			for i, kv := range getResp.Kvs {
				strs := strings.Split(string(kv.Key), "_")

				address := strs[1]
				score, err := strconv.ParseInt(string(kv.Value), 10, 64)
				if err != nil {
					srv.Logger.Error(err.Error())
					return "", err
				}

				scoreServer, err := slb.NewScoreServer(address, score, getResp.Header.Revision)
				if err != nil {
					srv.Logger.Error(err.Error())
					return "", err
				}

				scoreServers[i] = scoreServer
			}

			err = srv.SLB(name, scoreServers)
			if err == slb.ErrLocalCacheOff {
				address, err = srv.GetSLBAddressByScoreServers(scoreServers)
				if err != nil {
					srv.Logger.Error(err.Error())
					return "", err
				}
				return address, err
			} else if err != nil {
				srv.Logger.Error(err.Error())
				return "", err
			}

			go srv.EtcdWatchHandler(name)

			address, err = srv.ScoreSLB.GetSLBAddress(name)
			if err == slb.ErrLocalCacheOff {
				address, err = srv.GetSLBAddressByScoreServers(scoreServers)
				if err != nil {
					srv.Logger.Error(err.Error())
					return "", err
				}
				return address, err
			} else if err != nil {
				srv.Logger.Error(err.Error())
				return "", err
			}

		} else if err != nil {
			srv.Logger.Error(err.Error())
			return "", err
		}

		return address, nil
	}

	return srv.getSLBAddressWithoutCache(name)
}

// getSLBAddressWithoutCache get slb address without cache.
func (srv *EtcdService) getSLBAddressWithoutCache(name string) (address string, err error) {
	timeoutContext, cancel := context.WithTimeout(srv.Context, deadline)
	defer cancel()

	withPrefixOption := clientv3.WithPrefix()

	getKey := name + "_"

	getResp, err := srv.Client.Get(timeoutContext, getKey, withPrefixOption)
	if err != nil {
		srv.Logger.Error(err.Error())
		return "", err
	}

	scoreServers := make([]*slb.ScoreServer, len(getResp.Kvs))

	for i, kv := range getResp.Kvs {
		strs := strings.Split(string(kv.Key), "_")

		address := strs[1]
		score, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			srv.Logger.Error(err.Error())
			return "", err
		}

		scoreServer := new(slb.ScoreServer)
		scoreServer.Score = score
		scoreServer.Address = address
		scoreServers[i] = scoreServer
	}

	address, err = srv.GetSLBAddressByScoreServers(scoreServers)
	if err != nil {
		srv.Logger.Error(err.Error())
		return "", err
	}

	return address, err
}

// EtcdWatchHandler handle calls from etcd.
func (srv *EtcdService) EtcdWatchHandler(name string) {
	srv.RLock()
	if srv.Status != StatusTypeActive {
		srv.RUnlock()
		srv.Logger.Error(ErrServiceNotActive.Error(),
			zap.String("name", name),
		)
		return
	}
	srv.RUnlock()

	cancelContext, cancel := context.WithCancel(srv.Context)
	srv.SLBWatchCancel = cancel

	withPrefixOption := clientv3.WithPrefix()
	watchChan := srv.Client.Watch(cancelContext, name+"_", withPrefixOption)

	for {
		watchResp := <-watchChan
		if watchResp.Canceled {
			srv.Logger.Warn("register: watching has canceled",
				zap.String("name", name),
			)
			return
		}

		for _, event := range watchResp.Events {
			strs := strings.Split(string(event.Kv.Key), "_")

			name := strs[0]
			address := strs[1]
			var score int64

			if len(event.Kv.Value) > 0 {
				i, err := strconv.ParseInt(string(event.Kv.Value), 10, 64)
				if err != nil {
					srv.Logger.Error(err.Error())
					continue
				}
				score = i
			}

			scoreServer, err := slb.NewScoreServer(address, score, watchResp.Header.Revision)
			if err != nil {
				srv.Logger.Error(err.Error())
				continue
			}

			switch event.Type {
			case clientv3.EventTypePut:
				err = srv.PutScoreServer(name, scoreServer)
				if err != nil {
					srv.Logger.Error(err.Error())
					continue
				}
			case clientv3.EventTypeDelete:
				err = srv.DelScoreServer(name, scoreServer)
				if err == slb.ErrNoScoreServers {
					srv.Logger.Error(err.Error())
					cancel()
					continue
				} else if err != nil {
					srv.Logger.Error(err.Error())
					continue
				}
			}
		}

	}
}

// Deregister deregister this server.
func (srv *EtcdService) Deregister() {

	srv.Logger.Info("register: service deregistering...",
		zap.String("srv.Name", srv.Name),
	)

	srv.Lock()
	srv.Status = StatusTypeClosing
	srv.Unlock()

	// cancel watching.
	if srv.WatchCancel != nil {
		srv.WatchCancel()
	}

	// cancel slb watching.
	if srv.SLBWatchCancel != nil {
		srv.SLBWatchCancel()
	}

	deleteKey := srv.Name + "_" + srv.Address + "_"

	timeoutContext, cancel := context.WithTimeout(srv.Context, deadline)
	defer cancel()

	withPrefixOption := clientv3.WithPrefix()
	withPrevKVOption := clientv3.WithPrevKV()

	deleteResp, err := srv.Client.Delete(timeoutContext, deleteKey, withPrefixOption, withPrevKVOption)
	if err != nil {
		srv.Logger.Error(err.Error())
		return
	}

	for _, kv := range deleteResp.PrevKvs {
		srv.Logger.Info("register: etcd delete key",
			zap.ByteString("kv.Key", kv.Key),
			zap.ByteString("kv.Value", kv.Value),
			zap.Int64("deleteResp.Header.Revision", deleteResp.Header.Revision),
		)
	}

}
