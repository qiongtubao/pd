// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const pickedCountThreshold = 3

// healthyClient will wrap an etcd client and record its last health time.
// The etcd client inside will only maintain one connection to the etcd server
// to make sure each healthyClient could be used to check the health of a certain
// etcd endpoint without involving the load balancer of etcd client.
// healthyClient将包装一个etcd客户端并记录其最后的健康时间。
// 里面的etcd客户端只会与etcd服务器保持一个连接
// 确保每个healthyClient都可以用来检查某个客户端的健康状况
// etcd 端点，不涉及 etcd 客户端的负载均衡器。
type healthyClient struct {
	*clientv3.Client //etcd 客户端
	lastHealth  time.Time //最后健康的时间戳
	healthState prometheus.Gauge //数字指标 为了对外查询状态
	latency     prometheus.Observer //直方图向量指标
}

// healthChecker is used to check the health of etcd endpoints. Inside the checker,
// we will maintain a map from each available etcd endpoint to its healthyClient.
type healthChecker struct {
	source         string
	tickerInterval time.Duration
	tlsConfig      *tls.Config

	// Store as endpoint(string) -> *healthyClient
	healthyClients sync.Map
	// evictedEps records the endpoints which are evicted from the last health patrol,
	// the value is the count the endpoint being picked continuously after evicted.
	// Store as endpoint(string) -> pickedCount(int)
	evictedEps sync.Map
	// client is the etcd client the health checker is guarding, it will be set with
	// the checked healthy endpoints dynamically and periodically.
	client *clientv3.Client

	endpointCountState prometheus.Gauge
}

// initHealthChecker initializes the health checker for etcd client.
//初始化自动检查
func initHealthChecker(
	tickerInterval time.Duration,
	tlsConfig *tls.Config,
	client *clientv3.Client,
	source string,
) {
	healthChecker := &healthChecker{
		source:             source,
		tickerInterval:     tickerInterval,
		tlsConfig:          tlsConfig,
		client:             client,
		endpointCountState: etcdStateGauge.WithLabelValues(source, endpointLabel),
	}
	// A health checker has the same lifetime with the given etcd client.
	// 健康检查器与给定的 etcd 客户端具有相同的生命周期。
	ctx := client.Ctx()
	// Sync etcd endpoints and check the last health time of each endpoint periodically.
	// 同步etcd端点并定期检查每个端点的最后健康时间。
	go healthChecker.syncer(ctx)
	// Inspect the health of each endpoint by reading the health key periodically.
	// 通过定期读取健康密钥来检查每个端点的健康状况。
	go healthChecker.inspector(ctx)
}

//定时更新
func (checker *healthChecker) syncer(ctx context.Context) {
	defer logutil.LogPanic()
	checker.update()
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("etcd client is closed, exit the endpoint syncer goroutine",
				zap.String("source", checker.source))
			return
		case <-ticker.C:
			checker.update()
		}
	}
}

func (checker *healthChecker) inspector(ctx context.Context) {
	defer logutil.LogPanic()
	ticker := time.NewTicker(checker.tickerInterval)
	defer ticker.Stop()
	lastAvailable := time.Now()
	//一直循环
	for {
		select {
		case <-ctx.Done():
			//接受到ctx Done etcd客户端关闭，退出健康检查器goroutine
			log.Info("etcd client is closed, exit the health inspector goroutine",
				zap.String("source", checker.source))
			checker.close()
			return
		case <-ticker.C:
			//定时器触发 然后进行检查巡检状态
			lastEps, pickedEps, changed := checker.patrol(ctx)
			if len(pickedEps) == 0 {
				//重置端点
				// when no endpoint could be used, try to reset endpoints to update connect rather
				// than delete them to avoid there is no any endpoint in client.
				// Note: reset endpoints will trigger sub-connection closed, and then trigger reconnection.
				// Otherwise, the sub-connection will be retrying in gRPC layer and use exponential backoff,
				// and it cannot recover as soon as possible.
				// 当没有端点可以使用时，尝试重置端点以更新连接
				// 删除它们以避免客户端中没有任何端点。
				// 注意：重置端点会触发子连接关闭，然后触发重连。
				// 否则，子连接将在 gRPC 层重试并使用指数退避，
				// 并且无法尽快恢复。
				if time.Since(lastAvailable) > etcdServerDisconnectedTimeout {
					log.Info("no available endpoint, try to reset endpoints",
						zap.Strings("last-endpoints", lastEps),
						zap.String("source", checker.source))
					resetClientEndpoints(checker.client, lastEps...)
				}
				continue
			}
			if changed {
				oldNum, newNum := len(lastEps), len(pickedEps)
				//设置pd list
				checker.client.SetEndpoints(pickedEps...)
				//设置检查个数
				checker.endpointCountState.Set(float64(newNum))
				//变更pd 日志打印
				log.Info("update endpoints",
					zap.String("num-change", fmt.Sprintf("%d->%d", oldNum, newNum)),
					zap.Strings("last-endpoints", lastEps),
					zap.Strings("endpoints", checker.client.Endpoints()),
					zap.String("source", checker.source))
			}
			lastAvailable = time.Now()
		}
	}
}

func (checker *healthChecker) close() {
	checker.healthyClients.Range(func(key, value any) bool {
		healthyCli := value.(*healthyClient)
		healthyCli.healthState.Set(0)
		healthyCli.Client.Close()
		return true
	})
}

// Reset the etcd client endpoints to trigger reconnect.
func resetClientEndpoints(client *clientv3.Client, endpoints ...string) {
	client.SetEndpoints()
	client.SetEndpoints(endpoints...)
}

type healthProbe struct {
	ep   string
	took time.Duration
}

// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L105-L145
func (checker *healthChecker) patrol(ctx context.Context) ([]string, []string, bool) {
	var (
		count   = checker.clientCount()
		probeCh = make(chan healthProbe, count)
		wg      sync.WaitGroup
	)
	//遍历healthyClients key(string), value (healthyClient)
	checker.healthyClients.Range(func(key, value any) bool {
		wg.Add(1)
		go func(key, value any) {
			defer wg.Done()
			defer logutil.LogPanic()
			var (
				ep          = key.(string)
				healthyCli  = value.(*healthyClient)
				client      = healthyCli.Client
				healthState = healthyCli.healthState
				latency     = healthyCli.latency
				start       = time.Now()
			)
			// Check the health of the endpoint.
			healthy := IsHealthy(ctx, client)
			//记录耗时
			took := time.Since(start)
			latency.Observe(took.Seconds())
			if !healthy {
				healthState.Set(0)
				log.Warn("etcd endpoint is unhealthy",
					zap.String("endpoint", ep),
					zap.Duration("took", took),
					zap.String("source", checker.source))
				return
			}
			healthState.Set(1)
			// If the endpoint is healthy, update its last health time.
			// 如果端点健康，则更新其上次健康时间。
			checker.storeClient(ep, client, start)
			// Send the healthy probe result to the channel.
			// 将健康的探测结果发送到通道。
			probeCh <- healthProbe{ep, took}
		}(key, value)
		return true
	})
	wg.Wait()
	close(probeCh)
	var (
		lastEps   = checker.client.Endpoints()
		pickedEps = checker.pickEps(probeCh)
	)
	if len(pickedEps) > 0 {
		checker.updateEvictedEps(lastEps, pickedEps)
		pickedEps = checker.filterEps(pickedEps)
	}
	return lastEps, pickedEps, !typeutil.AreStringSlicesEquivalent(lastEps, pickedEps)
}

// Divide the acceptable latency range into several parts, and pick the endpoints which
// are in the first acceptable latency range. Currently, we only take the latency of the
// last health check into consideration, and maybe in the future we could introduce more
// factors to help improving the selection strategy.
func (checker *healthChecker) pickEps(probeCh <-chan healthProbe) []string {
	var (
		count     = len(probeCh)
		pickedEps = make([]string, 0, count)
	)
	if count == 0 {
		return pickedEps
	}
	// Consume the `probeCh` to build a reusable slice.
	probes := make([]healthProbe, 0, count)
	for probe := range probeCh {
		probes = append(probes, probe)
	}
	// Take the default value as an example, if we have 3 endpoints with latency like:
	//   - A: 175ms
	//   - B: 50ms
	//   - C: 2.5s
	// the distribution will be like:
	//   - [0, 1s) -> {A, B}
	//   - [1s, 2s)
	//   - [2s, 3s) -> {C}
	//   - ...
	//  - [9s, 10s)
	// Then the picked endpoints will be {A, B} and if C is in the last used endpoints, it will be evicted later.
	factor := int(DefaultRequestTimeout / DefaultSlowRequestTime)
	for i := 0; i < factor; i++ {
		minLatency, maxLatency := DefaultSlowRequestTime*time.Duration(i), DefaultSlowRequestTime*time.Duration(i+1)
		for _, probe := range probes {
			if minLatency <= probe.took && probe.took < maxLatency {
				log.Debug("pick healthy etcd endpoint within acceptable latency range",
					zap.Duration("min-latency", minLatency),
					zap.Duration("max-latency", maxLatency),
					zap.Duration("took", probe.took),
					zap.String("endpoint", probe.ep),
					zap.String("source", checker.source))
				pickedEps = append(pickedEps, probe.ep)
			}
		}
		if len(pickedEps) > 0 {
			break
		}
	}
	return pickedEps
}

func (checker *healthChecker) updateEvictedEps(lastEps, pickedEps []string) {
	// Create a set of picked endpoints for faster lookup
	pickedSet := make(map[string]bool, len(pickedEps))
	for _, ep := range pickedEps {
		pickedSet[ep] = true
	}
	// Reset the count to 0 if it's in evictedEps but not in the pickedEps.
	checker.evictedEps.Range(func(key, _ any) bool {
		ep := key.(string)
		if !pickedSet[ep] {
			checker.evictedEps.Store(ep, 0)
			log.Info("reset evicted etcd endpoint picked count",
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
		}
		return true
	})
	// Find all endpoints which are in the lastEps but not in the pickedEps,
	// and add them to the evictedEps.
	for _, ep := range lastEps {
		if pickedSet[ep] {
			continue
		}
		checker.evictedEps.Store(ep, 0)
		log.Info("evicted etcd endpoint found",
			zap.String("endpoint", ep),
			zap.String("source", checker.source))
	}
	// Find all endpoints which are in both pickedEps and evictedEps to
	// increase their picked count.
	for _, ep := range pickedEps {
		if count, ok := checker.evictedEps.Load(ep); ok {
			// Increase the count the endpoint being picked continuously.
			checker.evictedEps.Store(ep, count.(int)+1)
			log.Info("evicted etcd endpoint picked again",
				zap.Int("picked-count-threshold", pickedCountThreshold),
				zap.Int("picked-count", count.(int)+1),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
		}
	}
}

// Filter out the endpoints that are in evictedEps and have not been continuously picked
// for `pickedCountThreshold` times still, this is to ensure the evicted endpoints truly
// become available before adding them back to the client.
func (checker *healthChecker) filterEps(eps []string) []string {
	pickedEps := make([]string, 0, len(eps))
	for _, ep := range eps {
		if count, ok := checker.evictedEps.Load(ep); ok {
			if count.(int) < pickedCountThreshold {
				continue
			}
			checker.evictedEps.Delete(ep)
			log.Info("add evicted etcd endpoint back",
				zap.Int("picked-count-threshold", pickedCountThreshold),
				zap.Int("picked-count", count.(int)),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
		}
		pickedEps = append(pickedEps, ep)
	}
	// If the pickedEps is empty, it means all endpoints are evicted,
	// to gain better availability, just use the original picked endpoints.
	if len(pickedEps) == 0 {
		log.Warn("all etcd endpoints are evicted, use the picked endpoints directly",
			zap.Strings("endpoints", eps),
			zap.String("source", checker.source))
		return eps
	}
	return pickedEps
}

//定时检查长时间离线,长时间断开,多余的client清理
func (checker *healthChecker) update() {
	eps := checker.syncURLs()
	if len(eps) == 0 {
		log.Warn("no available etcd endpoint returned by etcd cluster",
			zap.String("source", checker.source))
		return
	}
	epMap := make(map[string]struct{}, len(eps))
	for _, ep := range eps {
		epMap[ep] = struct{}{}
	}
	// Check if client exists:
	//   - If not, create one.
	//   - If exists, check if it's offline or disconnected for a long time.
	for ep := range epMap {
		client := checker.loadClient(ep)
		if client == nil {
			//初始化client 添加到healthyClients
			checker.initClient(ep)
			continue
		}
		since := time.Since(client.lastHealth)
		// Check if it's offline for a long time and try to remove it.
		// 检查是否长时间离线，尝试删除。
		if since > etcdServerOfflineTimeout {
			log.Info("etcd server might be offline, try to remove it",
				zap.Duration("since-last-health", since),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
			checker.removeClient(ep)
			continue
		}
		// Check if it's disconnected for a long time and try to reconnect.
		// 检查是否长时间断开，然后尝试重新连接。
		if since > etcdServerDisconnectedTimeout {
			log.Info("etcd server might be disconnected, try to reconnect",
				zap.Duration("since-last-health", since),
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
			resetClientEndpoints(client.Client, ep)
		}
	}
	// Clean up the stale clients which are not in the etcd cluster anymore.
	// 清理不再位于 etcd 集群中的过时客户端。
	checker.healthyClients.Range(func(key, value any) bool {
		ep := key.(string)
		if _, ok := epMap[ep]; !ok {
			log.Info("remove stale etcd client",
				zap.String("endpoint", ep),
				zap.String("source", checker.source))
			checker.removeClient(ep)
		}
		return true
	})
}

func (checker *healthChecker) clientCount() int {
	count := 0
	checker.healthyClients.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (checker *healthChecker) loadClient(ep string) *healthyClient {
	if client, ok := checker.healthyClients.Load(ep); ok {
		return client.(*healthyClient)
	}
	return nil
}

func (checker *healthChecker) initClient(ep string) {
	//创建client
	client, err := newClient(checker.tlsConfig, ep)
	if err != nil {
		log.Error("failed to create etcd healthy client",
			zap.String("endpoint", ep),
			zap.String("source", checker.source),
			zap.Error(err))
		return
	}
	checker.storeClient(ep, client, time.Now())
}

func (checker *healthChecker) storeClient(ep string, client *clientv3.Client, lastHealth time.Time) {
	checker.healthyClients.Store(ep, &healthyClient{
		Client:      client,
		lastHealth:  lastHealth,
		healthState: etcdStateGauge.WithLabelValues(checker.source, ep),
		latency:     etcdEndpointLatency.WithLabelValues(checker.source, ep),
	})
}

func (checker *healthChecker) removeClient(ep string) {
	if client, ok := checker.healthyClients.LoadAndDelete(ep); ok {
		healthyCli := client.(*healthyClient)
		healthyCli.healthState.Set(0)
		if err := healthyCli.Close(); err != nil {
			log.Error("failed to close etcd healthy client",
				zap.String("endpoint", ep),
				zap.String("source", checker.source),
				zap.Error(err))
		}
	}
	checker.evictedEps.Delete(ep)
}

// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/clientv3/client.go#L170-L183
func (checker *healthChecker) syncURLs() (eps []string) {
	resp, err := ListEtcdMembers(clientv3.WithRequireLeader(checker.client.Ctx()), checker.client)
	if err != nil {
		log.Error("failed to list members",
			zap.String("source", checker.source),
			errs.ZapError(err))
		return nil
	}
	for _, m := range resp.Members {
		if len(m.Name) == 0 || m.IsLearner {
			continue
		}
		eps = append(eps, m.ClientURLs...)
	}
	return eps
}
