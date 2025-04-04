// Package rueidis is a fast Golang Redis RESP3 client that does auto pipelining and supports client side caching.
package rueidis

//go:generate go run hack/cmds/gen.go internal/cmds hack/cmds/*.json

import (
    "context"
    "crypto/tls"
    "errors"
    "math"
    "net"
    "runtime"
    "strings"
    "time"

    "github.com/redis/rueidis/internal/proto"
    "github.com/redis/rueidis/internal/util"
)

const (
    DefaultCacheBytes       = 128 * (1 << 20)
    DefaultRingScale        = 10
    DefaultPoolSize         = 1024
    DefaultBlockingPipeline = 2000
    DefaultDialTimeout      = 5 * time.Second
    DefaultTCPKeepAlive     = 1 * time.Second
    DefaultReadBuffer       = 1 << 19
    DefaultWriteBuffer      = 1 << 19
    MaxPipelineMultiplex    = 8
    ClientModeCluster       ClientMode = "cluster"
    ClientModeSentinel      ClientMode = "sentinel"
    ClientModeStandalone    ClientMode = "standalone"
)

var (
    ErrClosing                 = errors.New("rueidis client is closing or unable to connect redis")
    ErrNoAddr                  = errors.New("no alive address in InitAddress")
    ErrNoCache                 = errors.New("ClientOption.DisableCache must be true for redis not supporting client-side caching or not supporting RESP3")
    ErrRESP2PubSubMixed        = errors.New("rueidis does not support SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE mixed with other commands in RESP2")
    ErrBlockingPubSubMixed     = errors.New("rueidis does not support SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE mixed with other blocking commands")
    ErrDoCacheAborted          = errors.New("failed to fetch the cache because EXEC was aborted by redis or connection closed")
    ErrReplicaOnlyNotSupported = errors.New("ReplicaOnly is not supported for single client")
    ErrNoSendToReplicas        = errors.New("no SendToReplicas provided for standalone client with replicas")
    ErrWrongPipelineMultiplex  = errors.New("ClientOption.PipelineMultiplex must not be bigger than MaxPipelineMultiplex")
    ErrDedicatedClientRecycled = errors.New("dedicated client should not be used after recycled")
    ErrRedirectFailed          = errors.New("failed to follow REDIRECT response")
    DisableClientSetInfo       = make([]string, 0)
)

type ClientOption struct {
    TLSConfig             *tls.Config
    DialFn                func(string, *net.Dialer, *tls.Config) (conn net.Conn, err error)
    DialCtxFn             func(context.Context, string, *net.Dialer, *tls.Config) (conn net.Conn, err error)
    NewCacheStoreFn       NewCacheStoreFn
    OnInvalidations       func([]RedisMessage)
    SendToReplicas        func(cmd Completed) bool
    AuthCredentialsFn     func(AuthCredentialsContext) (AuthCredentials, error)
    RetryDelay            RetryDelayFn
    ReplicaSelector       func(slot uint16, replicas []ReplicaInfo) int
    Sentinel              SentinelOption
    Dialer                net.Dialer
    Username              string
    Password              string
    ClientName            string
    ClientSetInfo         []string
    InitAddress           []string
    ClientTrackingOptions []string
    Standalone            StandaloneOption
    SelectDB              int
    CacheSizeEachConn     int
    RingScaleEachConn     int
    ReadBufferEachConn    int
    WriteBufferEachConn   int
    BlockingPoolCleanup   time.Duration
    BlockingPoolMinSize   int
    BlockingPoolSize      int
    BlockingPipeline      int
    PipelineMultiplex     int
    ConnWriteTimeout      time.Duration
    MaxFlushDelay         time.Duration
    ClusterOption         ClusterOption
    DisableTCPNoDelay     bool
    ShuffleInit           bool
    ClientNoTouch         bool
    DisableRetry          bool
    DisableCache          bool
    DisableAutoPipelining bool
    AlwaysPipelining      bool
    AlwaysRESP2           bool
    ForceSingleClient     bool
    ReplicaOnly           bool
    ClientNoEvict         bool
    EnableReplicaAZInfo   bool
}

type SentinelOption struct {
    Dialer     net.Dialer
    TLSConfig  *tls.Config
    MasterSet  string
    Username   string
    Password   string
    ClientName string
}

type ClusterOption struct {
    ShardsRefreshInterval time.Duration
}

type StandaloneOption struct {
    ReplicaAddress []string
    EnableRedirect bool // Added for REDIRECT support
}

type ReplicaInfo struct {
    Addr string
    AZ   string
}

type ClientMode string

type Client interface {
    CoreClient
    DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp RedisResult)
    DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resp []RedisResult)
    DoStream(ctx context.Context, cmd Completed) RedisResultStream
    DoMultiStream(ctx context.Context, multi ...Completed) MultiRedisResultStream
    Dedicated(fn func(DedicatedClient) error) (err error)
    Dedicate() (client DedicatedClient, cancel func())
    Nodes() map[string]Client
    Mode() ClientMode
}

type DedicatedClient interface {
    CoreClient
    SetPubSubHooks(hooks PubSubHooks) <-chan error
}

type CoreClient interface {
    B() Builder
    Do(ctx context.Context, cmd Completed) (resp RedisResult)
    DoMulti(ctx context.Context, multi ...Completed) (resp []RedisResult)
    Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) error
    Close()
}

func CT(cmd Cacheable, ttl time.Duration) CacheableTTL {
    return CacheableTTL{Cmd: cmd, TTL: ttl}
}

type CacheableTTL struct {
    Cmd Cacheable
    TTL time.Duration
}

type AuthCredentialsContext struct {
    Address net.Addr
}

type AuthCredentials struct {
    Username string
    Password string
}

// Simplified types for demonstration (replace with actual internal/proto types)
type RedisResult = proto.Result
type Completed = proto.Command
type Cacheable = proto.Command
type RedisResultStream struct{}
type MultiRedisResultStream struct{}
type Builder interface {
    Get() *GetBuilder // Simplified; adjust based on actual Builder
}
type GetBuilder struct {
    cmd proto.Command
}

func (b *GetBuilder) Key(key string) *GetBuilder { b.cmd = proto.NewCommand([]string{"GET", key}); return b }
func (b *GetBuilder) Build() Completed           { return b.cmd }

type PubSubHooks struct{}
type PubSubMessage = proto.Message
type NewCacheStoreFn func(int) interface{}
type RetryDelayFn func(int) time.Duration

type conn struct {
    nc   net.Conn
    addr string
    opt  *ClientOption
    // Add internal fields (e.g., reader/writer) as per Rueidis
}

func newConn(ctx context.Context, addr string, opt *ClientOption) (*conn, error) {
    nc, err := dial(ctx, addr, opt)
    if err != nil {
        return nil, err
    }
    c := &conn{nc: nc, addr: addr, opt: opt}

    if opt.Standalone.EnableRedirect && opt.ForceSingleClient {
        cmd := proto.NewCommand([]string{"CLIENT", "CAPA", "redirect"})
        if err := c.writeCmd(cmd); err != nil {
            c.Close()
            return nil, err
        }
        resp, err := c.readResp()
        if err != nil || resp.Error() != nil {
            c.Close()
            return nil, err
        }
    }

    return c, nil
}

func (c *conn) writeCmd(cmd Completed) error {
    // Placeholder: Implement with actual proto.WriteCommand
    return c.nc.SetWriteDeadline(time.Now().Add(c.opt.ConnWriteTimeout))
}

func (c *conn) readResp() (RedisResult, error) {
    // Placeholder: Implement with actual proto.ReadResult
    return proto.Result{}, nil
}

func (c *conn) Close() {
    c.nc.Close()
}

type standaloneClient struct {
    conn *conn
    opt  *ClientOption
}

func (s *standaloneClient) Do(ctx context.Context, cmd Completed) RedisResult {
    if err := s.conn.writeCmd(cmd); err != nil {
        return proto.NewErrResult(err)
    }

    resp, err := s.conn.readResp()
    if err != nil {
        return proto.NewErrResult(err)
    }
    if s.opt.Standalone.EnableRedirect && s.isRedirect(resp) {
        newAddr, err := s.redirectAddr(resp)
        if err != nil {
            return proto.NewErrResult(err)
        }
        newConn, err := newConn(ctx, newAddr, s.opt)
        if err != nil {
            return proto.NewErrResult(ErrRedirectFailed)
        }
        s.conn.Close()
        s.conn = newConn
        if err := s.conn.writeCmd(cmd); err != nil {
            return proto.NewErrResult(err)
        }
        resp, err = s.conn.readResp()
        if err != nil {
            return proto.NewErrResult(err)
        }
    }
    return resp
}

func (s *standaloneClient) isRedirect(resp RedisResult) bool {
    msg, err := resp.ToMessage()
    if err != nil || msg.Type != proto.Array {
        return false
    }
    if len(msg.Values) != 2 || msg.Values[0].String != "REDIRECT" {
        return false
    }
    return true
}

func (s *standaloneClient) redirectAddr(resp RedisResult) (string, error) {
    msg, _ := resp.ToMessage()
    if len(msg.Values) != 2 {
        return "", errors.New("invalid REDIRECT response")
    }
    return msg.Values[1].String, nil
}

func (s *standaloneClient) B() Builder {
    return &builderImpl{client: s}
}

type builderImpl struct {
    client *standaloneClient
}

func (b *builderImpl) Get() *GetBuilder { return &GetBuilder{} }

func (s *standaloneClient) DoMulti(ctx context.Context, multi ...Completed) []RedisResult { return nil }
func (s *standaloneClient) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) error {
    return nil
}
func (s *standaloneClient) Close()                           {}
func (s *standaloneClient) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) RedisResult {
    return proto.Result{}
}
func (s *standaloneClient) DoMultiCache(ctx context.Context, multi ...CacheableTTL) []RedisResult {
    return nil
}
func (s *standaloneClient) DoStream(ctx context.Context, cmd Completed) RedisResultStream {
    return RedisResultStream{}
}
func (s *standaloneClient) DoMultiStream(ctx context.Context, multi ...Completed) MultiRedisResultStream {
    return MultiRedisResultStream{}
}
func (s *standaloneClient) Dedicated(fn func(DedicatedClient) error) error { return nil }
func (s *standaloneClient) Dedicate() (DedicatedClient, func())           { return nil, nil }
func (s *standaloneClient) Nodes() map[string]Client                     { return nil }
func (s *standaloneClient) Mode() ClientMode                            { return ClientModeStandalone }

func NewClient(option ClientOption) (client Client, err error) {
    if option.ReadBufferEachConn < 32 {
        option.ReadBufferEachConn = DefaultReadBuffer
    }
    if option.WriteBufferEachConn < 32 {
        option.WriteBufferEachConn = DefaultWriteBuffer
    }
    if option.CacheSizeEachConn <= 0 {
        option.CacheSizeEachConn = DefaultCacheBytes
    }
    if option.Dialer.Timeout == 0 {
        option.Dialer.Timeout = DefaultDialTimeout
    }
    if option.Dialer.KeepAlive == 0 {
        option.Dialer.KeepAlive = DefaultTCPKeepAlive
    }
    if option.ConnWriteTimeout == 0 {
        option.ConnWriteTimeout = max(DefaultTCPKeepAlive, option.Dialer.KeepAlive) * 10
    }
    if option.BlockingPipeline == 0 {
        option.BlockingPipeline = DefaultBlockingPipeline
    }
    if option.DisableAutoPipelining {
        option.AlwaysPipelining = false
    }
    if option.ShuffleInit {
        util.Shuffle(len(option.InitAddress), func(i, j int) {
            option.InitAddress[i], option.InitAddress[j] = option.InitAddress[j], option.InitAddress[i]
        })
    }
    if option.PipelineMultiplex > MaxPipelineMultiplex {
        return nil, ErrWrongPipelineMultiplex
    }
    if option.RetryDelay == nil {
        option.RetryDelay = defaultRetryDelayFn
    }
    if option.Sentinel.MasterSet != "" {
        option.PipelineMultiplex = singleClientMultiplex(option.PipelineMultiplex)
        return newSentinelClient(&option, makeConn, newRetryer(option.RetryDelay))
    }
    if len(option.Standalone.ReplicaAddress) > 0 {
        if option.SendToReplicas == nil {
            return nil, ErrNoSendToReplicas
        }
        option.PipelineMultiplex = singleClientMultiplex(option.PipelineMultiplex)
        return newStandaloneClient(&option, makeConn, newRetryer(option.RetryDelay))
    }
    if option.ForceSingleClient {
        option.PipelineMultiplex = singleClientMultiplex(option.PipelineMultiplex)
        return newSingleClient(&option, nil, makeConn, newRetryer(option.RetryDelay))
    }
    if client, err = newClusterClient(&option, makeConn, newRetryer(option.RetryDelay)); err != nil {
        if client == (*clusterClient)(nil) {
            return nil, err
        }
        if len(option.InitAddress) == 1 && (err.Error() == redisErrMsgCommandNotAllow || strings.Contains(strings.ToUpper(err.Error()), "CLUSTER")) {
            option.PipelineMultiplex = singleClientMultiplex(option.PipelineMultiplex)
            client, err = newSingleClient(&option, client.(*clusterClient).single(), makeConn, newRetryer(option.RetryDelay))
        } else {
            client.Close()
            return nil, err
        }
    }
    return client, err
}

func singleClientMultiplex(multiplex int) int {
    if multiplex == 0 {
        if multiplex = int(math.Log2(float64(runtime.GOMAXPROCS(0)))); multiplex >= 2 {
            multiplex = 2
        }
    }
    if multiplex < 0 {
        multiplex = 0
    }
    return multiplex
}

func makeConn(dst string, opt *ClientOption) conn {
    return makeMux(dst, opt, dial)
}

func dial(ctx context.Context, dst string, opt *ClientOption) (conn net.Conn, err error) {
    if opt.DialCtxFn != nil {
        return opt.DialCtxFn(ctx, dst, &opt.Dialer, opt.TLSConfig)
    }
    if opt.DialFn != nil {
        return opt.DialFn(dst, &opt.Dialer, opt.TLSConfig)
    }
    if opt.TLSConfig != nil {
        dialer := tls.Dialer{NetDialer: &opt.Dialer, Config: opt.TLSConfig}
        conn, err = dialer.DialContext(ctx, "tcp", dst)
    } else {
        conn, err = opt.Dialer.DialContext(ctx, "tcp", dst)
    }
    return conn, err
}

const redisErrMsgCommandNotAllow = "command is not allowed"

func max(a, b time.Duration) time.Duration { if a > b { return a } return b }
func defaultRetryDelayFn(attempt int) time.Duration { return time.Second }
func newSentinelClient(opt *ClientOption, connFn func(string, *ClientOption) conn, retryer interface{}) (Client, error) {
    return nil, nil
}
func newStandaloneClient(opt *ClientOption, connFn func(string, *ClientOption) conn, retryer interface{}) (Client, error) {
    ctx := context.Background()
    c, err := newConn(ctx, opt.InitAddress[0], opt)
    if err != nil {
        return nil, err
    }
    return &standaloneClient{conn: c, opt: opt}, nil
}
func newSingleClient(opt *ClientOption, single *singleClient, connFn func(string, *ClientOption) conn, retryer interface{}) (Client, error) {
    return nil, nil
}
func newClusterClient(opt *ClientOption, connFn func(string, *ClientOption) conn, retryer interface{}) (Client, error) {
    return nil, nil
}
func makeMux(dst string, opt *ClientOption, dialFn func(context.Context, string, *ClientOption) (net.Conn, error)) conn {
    ctx := context.Background()
    nc, _ := dialFn(ctx, dst, opt)
    return conn{nc: nc, addr: dst, opt: opt}
}
func newRetryer(fn RetryDelayFn) interface{} { return nil }
type clusterClient struct{}
func (c *clusterClient) single() *singleClient { return nil }
type singleClient struct{}