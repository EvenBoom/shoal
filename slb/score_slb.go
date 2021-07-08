package slb

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

/*
This Server Load Balancing works by score-polling.
*/

// errors.
var (
	ErrLocalCacheOff      = errors.New("slb: local cache off")
	ErrNoScoreServers     = errors.New("slb: no score servers")
	ErrEmptyServiceName   = errors.New("slb: service name is empty")
	ErrEmptyServerAddress = errors.New("slb: server address is empty")
	ErrZeroServerScore    = errors.New("slb: server score is zero")
	ErrLowVersion         = errors.New("slb: version is low")
)

// ScoreServer score server.
type ScoreServer struct {
	Version  int64  // etcd version, default value is -1.
	Address  string // server address.
	Score    int64  // score must be larger than 0.
	SLBScore int64  // server load balancing score.
}

// OptType operating type of score-server
type OptType string

// type of score-server operating
const (
	OptTypeModify OptType = "modify"
	OptTypeDelete OptType = "delete"
	OptTypeCreate OptType = "create"
)

// ScoreSLB score server load balancing.
// LocalCache if each server has too many services, it is better when not use local cache.
type ScoreSLB struct {
	ScoreServers map[string][]*ScoreServer // score servers, key was service name.
	LocalCache   bool                      // use local cache.
	sync.RWMutex                           // read-write mutex.
}

// SetLocalCache set local cache.
// release score servers when LocalCache is false.
func (slb *ScoreSLB) SetLocalCache(localCache bool) (address string, err error) {

	slb.Lock()
	defer slb.Unlock()

	slb.LocalCache = localCache
	if !slb.LocalCache {
		slb.ScoreServers = nil
	}

	return
}

// GetSLBAddress get address after server load balancing.
// get slb address when LocalCache is true.
func (slb *ScoreSLB) GetSLBAddress(name string) (address string, err error) {

	if name == "" {
		return "", ErrEmptyServiceName
	}

	var lastScoreServer *ScoreServer

	slb.RLock()

	if !slb.LocalCache {
		slb.RUnlock()
		return "", ErrLocalCacheOff
	}

	scoreServers := slb.ScoreServers[name]
	slb.RUnlock()

	if len(scoreServers) == 0 {
		return "", ErrNoScoreServers
	}

	scoreServersLen := len(scoreServers)

	if scoreServersLen > 0 {
		lastScoreServer = scoreServers[scoreServersLen-1]
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomScore := random.Int63n(lastScoreServer.SLBScore)

	for _, scoreServer := range scoreServers {
		if randomScore < scoreServer.SLBScore {
			address = scoreServer.Address
			break
		}
	}

	return
}

// GetSLBAddressByScoreServers get address after server load balancing.
func (slb *ScoreSLB) GetSLBAddressByScoreServers(scoreServers []*ScoreServer) (address string, err error) {

	if len(scoreServers) == 0 {
		return "", ErrNoScoreServers
	}

	var sumScore int64 = 0

	for _, scoreServer := range scoreServers {

		if scoreServer.Score == 0 {
			return "", ErrZeroServerScore
		}

		sumScore += scoreServer.Score
		scoreServer.SLBScore = sumScore
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomScore := random.Int63n(sumScore)

	for _, scoreServer := range scoreServers {
		if randomScore < scoreServer.SLBScore {
			address = scoreServer.Address
			break
		}
	}

	return
}

// NewScoreSLB make a score server load balancing.
func NewScoreSLB() *ScoreSLB {
	return new(ScoreSLB)
}

// NewScoreServer make a score server.
func NewScoreServer(address string, score, version int64) (scoreServer *ScoreServer, err error) {

	if address == "" {
		return nil, ErrEmptyServerAddress
	}

	if score < 1 {
		score = 1
	}

	if version < 0 {
		version = -1
	}

	scoreServer = new(ScoreServer)
	scoreServer.Address = address
	scoreServer.Score = score
	scoreServer.Version = version

	return
}

// SLB server load balancing.
// if score smaller than 1, will set 1.
// before use this func, must new ScoreSLB first.
func (slb *ScoreSLB) SLB(name string, scoreServers []*ScoreServer) error {

	if name == "" {
		return ErrEmptyServiceName
	}

	if len(scoreServers) == 0 {
		return ErrNoScoreServers
	}

	slb.Lock()
	defer slb.Unlock()

	if !slb.LocalCache {
		return ErrLocalCacheOff
	}

	var score int64 = 0

	for _, scoreServer := range scoreServers {

		if scoreServer.Score == 0 {
			return ErrZeroServerScore
		}

		score += scoreServer.Score
		scoreServer.SLBScore = score

	}

	if slb.ScoreServers == nil {
		slb.ScoreServers = make(map[string][]*ScoreServer)
	}

	// set score servers.
	slb.ScoreServers[name] = scoreServers

	return nil
}

// PutScoreServer put score server
func (slb *ScoreSLB) PutScoreServer(name string, scoreServer *ScoreServer) error {
	if scoreServer.Score == 0 {
		return ErrZeroServerScore
	}

	slb.Lock()
	defer slb.Unlock()

	scoreServers := slb.ScoreServers[name]

	var score int64
	var hasPut bool

	for _, server := range scoreServers {

		server.SLBScore += score

		if server.Address == scoreServer.Address {
			if server.Version >= scoreServer.Version {
				return ErrLowVersion
			}

			score = scoreServer.Score - server.Score
			scoreServer.SLBScore = server.SLBScore + score

			server = scoreServer

			hasPut = true
		}

	}

	if hasPut {
		return nil
	}

	scoreServer.SLBScore = scoreServer.Score

	if len(scoreServers) > 0 {
		scoreServer.SLBScore += scoreServers[len(scoreServers)-1].SLBScore
	}

	scoreServers = append(scoreServers, scoreServer)

	slb.ScoreServers[name] = scoreServers

	return nil
}

// DelScoreServer delete score server
func (slb *ScoreSLB) DelScoreServer(name string, scoreServer *ScoreServer) (err error) {

	slb.Lock()
	defer slb.Unlock()

	scoreServers := slb.ScoreServers[name]
	len := len(scoreServers)

	var score int64
	var hasDel bool

	for i, server := range scoreServers {

		if hasDel {
			server.SLBScore -= score
			scoreServers[i-1] = server
		}

		if server.Address == scoreServer.Address {
			if server.Version >= scoreServer.Version {
				return ErrLowVersion
			}

			score = server.Score
			hasDel = true
		}

	}

	if hasDel {
		if len > 1 {
			slb.ScoreServers[name] = scoreServers[:len-1]
		} else {
			slb.ScoreServers[name] = nil
			return ErrNoScoreServers
		}

	}

	return nil
}
