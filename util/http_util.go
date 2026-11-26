package util

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/http/httpproxy"
	"pansou/config"
)

// 全局HTTP客户端
var httpClient *http.Client

// TG 频道抓取专用客户端（连接数与拨号策略都与其他上游不同，见 GetTGHTTPClient）
var tgHTTPClient *http.Client

// InitHTTPClient 初始化HTTP客户端
func InitHTTPClient() {
	proxyURL := ""
	if config.AppConfig != nil {
		proxyURL = config.AppConfig.ProxyURL
	}

	client, err := NewHTTPClient(proxyURL)
	if err != nil {
		client, _ = NewHTTPClient("")
	}
	httpClient = client

	// 插件走的是 http.DefaultTransport，也要一并调优，否则插件路径仍然
	// 每主机只保留 2 条空闲连接。
	TuneDefaultTransport()
}

// upstreamPoolSettings 返回上游连接池设置，供共享客户端与 DefaultTransport 共用。
func upstreamPoolSettings() (int, time.Duration) {
	maxIdleConnsPerHost := 20
	idleConnTimeout := 90 * time.Second
	if config.AppConfig != nil {
		if config.AppConfig.UpstreamMaxIdleConnsPerHost > 0 {
			maxIdleConnsPerHost = config.AppConfig.UpstreamMaxIdleConnsPerHost
		}
		if config.AppConfig.UpstreamIdleConnTimeout > 0 {
			idleConnTimeout = config.AppConfig.UpstreamIdleConnTimeout
		}
	}
	return maxIdleConnsPerHost, idleConnTimeout
}

// TuneDefaultTransport 调优 http.DefaultTransport 的连接池。
//
// plugin/ 下 77 个文件、104 处自建 &http.Client{}，没有任何插件使用共享客户端；
// 而插件框架自己给的客户端也是 &http.Client{Timeout: ...}（Transport 为 nil），
// 它们全都落在 http.DefaultTransport 上。DefaultTransport 默认每主机只保留
// 2 条空闲连接：并发突发结束后多余连接立刻被关闭，下一轮搜索必须重新做
// TCP+TLS 握手。实测（5 轮 × 8 并发，同一主机）新建连接 32 条降至 8 条。
//
// 除了连接池，这里还要统一代理解析：DefaultTransport 自带的
// ProxyFromEnvironment 只识别 HTTP_PROXY/HTTPS_PROXY/NO_PROXY，不认识
// 本项目文档中的 PROXY，也不认识 ALL_PROXY。插件路径恰恰全走
// DefaultTransport，所以只设置 PROXY 或 ALL_PROXY 时插件会整体退回直连
// （TG 频道路径走共享客户端，不受影响）。详见 BuildProxyFunc。
func TuneDefaultTransport() {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return
	}
	maxIdleConnsPerHost, idleConnTimeout := upstreamPoolSettings()
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
	transport.MaxIdleConns = max(100, maxIdleConnsPerHost)
	transport.IdleConnTimeout = idleConnTimeout

	rawProxyURL := ""
	if config.AppConfig != nil {
		rawProxyURL = config.AppConfig.ProxyURL
	}
	if err := applyProxyFunc(transport, rawProxyURL); err != nil {
		fmt.Printf("[HTTP] 代理解析失败，插件路径退回标准环境变量代理: %v\n", err)
	}

	fmt.Printf("[HTTP] 已调优 DefaultTransport：每主机空闲连接 %d，保活 %v，代理=%s（插件均使用该连接池）\n",
		maxIdleConnsPerHost, idleConnTimeout, describeProxy(rawProxyURL))
}

// describeProxy 用于启动日志，避免把含账号密码的代理地址整条打出来。
// MaskProxyURL 去掉代理地址里的用户名密码后返回，用于日志。
//
// 代理地址常写成 http://user:pass@host:port，直接打印等于把凭据写进日志与容器
// 日志采集链路（main.go 启动时原先就是这么打的）。解析不出来时返回占位文案而不是
// 原文——拿不准就宁可不打。
func MaskProxyURL(rawProxyURL string) string {
	trimmed := strings.TrimSpace(rawProxyURL)
	if trimmed == "" {
		return "未配置"
	}
	u, err := url.Parse(trimmed)
	if err != nil || u.Host == "" {
		return "已配置代理（地址无法解析，已隐藏）"
	}
	if u.User != nil {
		u.User = nil
		return u.String() + "（凭据已隐藏）"
	}
	return u.String()
}

func describeProxy(rawProxyURL string) string {
	if strings.TrimSpace(rawProxyURL) == "" {
		return "标准环境变量(HTTP_PROXY/HTTPS_PROXY/NO_PROXY)"
	}
	return "已配置代理"
}

// NewHTTPClient 创建HTTP客户端，可按需指定本客户端使用的代理。
func NewHTTPClient(proxyURL string) (*http.Client, error) {
	return newHTTPClient(proxyURL, 100, nil)
}

// newHTTPClient 是共享客户端与 TG 专用客户端的共同构造入口。
// maxConnsPerHost 为 0 时沿用 100（历史行为）；dial 为 nil 时使用标准双栈拨号。
func newHTTPClient(proxyURL string, maxConnsPerHost int, dial func(context.Context, string, string) (net.Conn, error)) (*http.Client, error) {
	// 连接池配置：空闲连接保活时间直接影响"快速兜底"这类密集访问路径的
	// 冷启动成本——连接一旦被回收，下次搜索要多付一次 TCP+TLS 握手。
	maxIdleConnsPerHost, idleConnTimeout := upstreamPoolSettings()
	if maxConnsPerHost <= 0 {
		maxConnsPerHost = 100
	}
	maxIdleConns := 100
	if maxIdleConns < maxIdleConnsPerHost {
		maxIdleConns = maxIdleConnsPerHost
	}

	if dial == nil {
		dial = (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
	}

	// 创建传输配置
	transport := &http.Transport{
		// 启用HTTP/2
		ForceAttemptHTTP2: true,

		// TLS配置
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false, // 生产环境应设为false
		},

		// 连接池优化
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		// TCP连接优化
		DialContext: dial,
	}

	if err := applyProxyFunc(transport, proxyURL); err != nil {
		return nil, err
	}

	// 创建客户端
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(60) * time.Second,
	}

	return client, nil
}

// GetTGHTTPClient 返回 TG 频道抓取专用客户端。
//
// 与共享客户端只有两点不同，都是针对 t.me 的实测结论（2026-11-25，生产机：
// 111 个频道 × 每页 gzip 后约 22KB）：
//
//  1. **每主机连接数压到 8**。共享客户端是 100 条，111 个频道会同时新建上百条
//     TCP+TLS 连接；4 秒预算下只有 18~58/111 能完成，其余全部挂到超时，
//     日志就是 `[searchTG] 成功 0/111，超时未完成 108`。t.me 已确认 ALPN 协商 h2，
//     8 条连接足以复用全部请求：同样 4 秒预算下 108/111，8 秒预算下 111/111。
//  2. **优先 IPv4 拨号**。同一台机器上 IPv4 访问 t.me 20/20 成功，IPv6 只有 13/20，
//     失败都是建连阶段无响应（`read tcp [2404:...]`、connect 0.000s）。默认先走
//     IPv4，2 秒内没连上再回落双栈，IPv6 仍然保留为可用路径，纯 IPv6 环境不受影响。
func GetTGHTTPClient() *http.Client {
	if tgHTTPClient != nil {
		return tgHTTPClient
	}
	proxyURL := ""
	family := "v4"
	maxConns := 8
	if config.AppConfig != nil {
		proxyURL = config.AppConfig.ProxyURL
		if config.AppConfig.TGMaxConnsPerHost > 0 {
			maxConns = config.AppConfig.TGMaxConnsPerHost
		}
		if config.AppConfig.TGIPFamily != "" {
			family = config.AppConfig.TGIPFamily
		}
	}

	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	var dial func(context.Context, string, string) (net.Conn, error) = dialer.DialContext
	if family != "auto" {
		dial = preferIPv4Dial(dialer)
	}

	client, err := newHTTPClient(proxyURL, maxConns, dial)
	if err != nil {
		// 代理配置不合法时退回不带代理的 TG 客户端，保持与 InitHTTPClient 一致的兜底。
		client, _ = newHTTPClient("", maxConns, dial)
	}
	tgHTTPClient = client
	return tgHTTPClient
}

// tgIPv4AttemptBudget 是"先 IPv4"策略里单次 IPv4 建连的预算。
// 超过它就放弃 IPv4、回落到双栈，避免 IPv4 被静默丢包时把整轮预算耗死。
// 实测到 t.me 的正常建连约 0.2 秒，2 秒足够覆盖抖动。
const tgIPv4AttemptBudget = 2 * time.Second

// preferIPv4Dial 返回一个优先 IPv4 的拨号函数：先只拨 tcp4，成功就直接用；
// 失败（含目标只有 AAAA 记录）或超过 tgIPv4AttemptBudget 时回落到双栈，
// IPv6 仍然保留为可用路径，纯 IPv6 环境不受影响。
func preferIPv4Dial(d *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" {
			return d.DialContext(ctx, network, addr)
		}

		ctx4, cancel4 := context.WithTimeout(ctx, tgIPv4AttemptBudget)
		conn, err := d.DialContext(ctx4, "tcp4", addr)
		cancel4()
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		return d.DialContext(ctx, "tcp", addr)
	}
}

// BuildProxyFunc 构造统一的代理解析函数。
//
// 代理来源优先级由 config.getProxyURL 决定：PROXY 优先，其次兼容
// HTTPS_PROXY/HTTP_PROXY/ALL_PROXY。这里必须显式构造，不能依赖
// http.ProxyFromEnvironment——它只识别 HTTP_PROXY/HTTPS_PROXY/NO_PROXY，
// 不认识本项目文档中的 PROXY，也不认识 ALL_PROXY。插件路径走的是
// http.DefaultTransport，此前正是因此漏掉了这两种配置：
// 只设置 PROXY 或 ALL_PROXY 时，插件会全部退回直连。
//
// 显式配置代理时按 NO_PROXY 放行直连；未配置任何代理时交回
// http.ProxyFromEnvironment，保持标准语义。
func BuildProxyFunc(rawProxyURL string, noProxy string) (func(*url.URL) (*url.URL, error), error) {
	raw := strings.TrimSpace(rawProxyURL)
	if raw == "" {
		// 保持标准语义，含 ProxyFromEnvironment 对环境变量的单次缓存行为
		return func(u *url.URL) (*url.URL, error) {
			return http.ProxyFromEnvironment(&http.Request{URL: u})
		}, nil
	}

	normalized, err := normalizeProxyURL(raw)
	if err != nil {
		return nil, err
	}

	cfg := &httpproxy.Config{
		HTTPProxy:  normalized,
		HTTPSProxy: normalized,
		NoProxy:    strings.TrimSpace(noProxy),
	}
	return cfg.ProxyFunc(), nil
}

// normalizeProxyURL 校验并归一化代理地址。
// socks5h 归一化为 socks5：Transport 原生支持 socks5，两者都本地解析域名。
func normalizeProxyURL(raw string) (string, error) {
	proxyURL, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("代理地址解析失败: %w", err)
	}
	if proxyURL.Scheme == "" || proxyURL.Host == "" {
		return "", fmt.Errorf("代理地址必须包含协议和主机")
	}

	switch strings.ToLower(proxyURL.Scheme) {
	case "socks5", "socks5h":
		clone := *proxyURL
		clone.Scheme = "socks5"
		return clone.String(), nil
	case "http", "https":
		return proxyURL.String(), nil
	default:
		return "", fmt.Errorf("不支持的代理协议: %s", proxyURL.Scheme)
	}
}

// ProxyFuncForTransport 返回可直接赋给 http.Transport.Proxy 的函数，按项目统一
// 规则解析代理（PROXY -> HTTPS_PROXY -> HTTP_PROXY -> ALL_PROXY，并按 NO_PROXY 放行直连）。
//
// 供插件手写 &http.Transport{...} 时使用。手写 Transport 的 Proxy 字段是零值，
// 含义是"永远直连"——只要部署方配了代理，这些插件就会整体失败，
// 而默认的 http.DefaultTransport 已经由 TuneDefaultTransport 接好，两者行为不一致。
func ProxyFuncForTransport() func(*http.Request) (*url.URL, error) {
	rawProxyURL := ""
	if config.AppConfig != nil {
		rawProxyURL = config.AppConfig.ProxyURL
	}
	proxyFunc, err := BuildProxyFunc(rawProxyURL, configuredNoProxy())
	if err != nil {
		fmt.Printf("[HTTP] 代理解析失败，该 Transport 退回标准环境变量代理: %v\n", err)
		return http.ProxyFromEnvironment
	}
	return func(req *http.Request) (*url.URL, error) {
		return proxyFunc(req.URL)
	}
}

// applyProxyFunc 把统一解析出的代理解析函数装到 transport 上。
// Transport.Proxy 的签名是 func(*http.Request)，httpproxy 给的是
// func(*url.URL)，这里做一次适配。
func applyProxyFunc(transport *http.Transport, rawProxyURL string) error {
	proxyFunc, err := BuildProxyFunc(rawProxyURL, configuredNoProxy())
	if err != nil {
		return err
	}
	transport.Proxy = func(req *http.Request) (*url.URL, error) {
		return proxyFunc(req.URL)
	}
	return nil
}

func configuredNoProxy() string {
	if config.AppConfig != nil {
		return config.AppConfig.NoProxy
	}
	return ""
}

// GetHTTPClient 获取HTTP客户端
func GetHTTPClient() *http.Client {
	if httpClient == nil {
		InitHTTPClient()
	}
	return httpClient
}

// FetchHTML 获取HTML内容
func FetchHTML(targetURL string) (string, error) {
	// 使用优化后的HTTP客户端
	client := GetHTTPClient()

	// 创建请求
	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		return "", err
	}

	// 设置请求头
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// 读取响应体（上游量级由对方决定，必须封顶；本包内直接调用）
	body, err := ReadAllLimited(resp.Body, MaxUpstreamResponseBytes)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// BuildSearchURL 构建搜索URL
func BuildSearchURL(channel string, keyword string, nextPageParam string) string {
	// channel 直接来自请求参数，必须转义：否则可注入 ? / # 与空格，污染路径或
	// 查询串（同函数下面的 keyword 是转义过的，channel 漏了）。合法频道名只有
	// 字母数字下划线，PathEscape 对它们是无操作。
	baseURL := "https://t.me/s/" + url.PathEscape(channel)
	if keyword != "" {
		baseURL += "?q=" + url.QueryEscape(keyword)
		if nextPageParam != "" {
			baseURL += "&" + nextPageParam
		}
	}
	return baseURL
}
