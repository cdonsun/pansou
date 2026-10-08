// Package guohe 果核剥壳（www.ghxi.com）插件。
//
// 站点为 WordPress 文章站，以精品软件分享为主，详情页含夸克、迅雷、UC、
// 光垒云盘等下载链接。搜索流程：GET /?s={kw} 解析文章列表 → 并发抓取详情页 →
// 提取网盘链接与提取码 → 转换为标准 SearchResult。
package guohe

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"

	"pansou/model"
	"pansou/plugin"
	"pansou/util"
)

const (
	pluginName      = "guohe"
	defaultPriority = 2

	// WebsiteURL 站点首页（搜索页为 /?s={kw}，ghpym.com 已 301 到 ghxi.com）
	WebsiteURL = "https://www.ghxi.com"

	requestTimeout   = 12 * time.Second
	detailTimeout    = 10 * time.Second
	maxListItems     = 10
	maxDetailWorkers = 6

	httpMaxIdleConns    = 64
	httpMaxIdlePerHost  = 16
	httpMaxConnsPerHost = 32

	retryBaseDelay    = 200 * time.Millisecond
	maxRequestRetries = 2
)

// 网盘链接识别：与前端 CLOUD_TYPES 的键保持一致（other 兜底）。
var linkPatterns = []struct {
	reg *regexp.Regexp
	typ string
}{
	{regexp.MustCompile(`https?://pan\.quark\.cn/(?:s|g)/[0-9A-Za-z]+`), "quark"},
	{regexp.MustCompile(`https?://pan\.xunlei\.com/s/[0-9A-Za-z\-_]+`), "xunlei"},
	{regexp.MustCompile(`https?://pan\.baidu\.com/s/[0-9A-Za-z\-_]+`), "baidu"},
	{regexp.MustCompile(`https?://(?:www\.)?(aliyundrive\.com|alipan\.com)/s/[0-9A-Za-z]+`), "aliyun"},
	{regexp.MustCompile(`https?://drive\.uc\.cn/s/[0-9A-Za-z]+`), "uc"},
	{regexp.MustCompile(`https?://(?:www\.)?(123pan\.com|123pan\.cn|123684\.com|123685\.com|123912\.com|123592\.com|pan\.123pan\.com)/s/[0-9A-Za-z]+`), "123"},
	{regexp.MustCompile(`https?://(?:www\.)?mypikpak\.com/s/[0-9A-Za-z]+`), "pikpak"},
	{regexp.MustCompile(`https?://caiyun\.139\.com/[^\s<>"']+`), "mobile"},
	{regexp.MustCompile(`https?://cloud\.189\.cn/[^\s<>"']+`), "tianyi"},
	{regexp.MustCompile(`https?://(?:www\.)?115\.com/s/[0-9A-Za-z]+`), "115"},
	{regexp.MustCompile(`https?://(?:www\.)?guangyapan\.com/s/[0-9A-Za-z\-_]+`), "other"},
	{regexp.MustCompile(`https?://[^\s<>"']*\.ctfile\.com/[^\s<>"']*`), "other"},
}

// 提取码模式：优先取链接 ?pwd= 参数，其次匹配页面文案。
var passwordPatterns = []*regexp.Regexp{
	regexp.MustCompile(`提取码[:：]?\s*([0-9A-Za-z]{4})`),
	regexp.MustCompile(`密码[:：]?\s*([0-9A-Za-z]{4})`),
	regexp.MustCompile(`(?:pwd|passcode)[:：=]\s*([0-9A-Za-z]{4})`),
}

// 发布时间模式（WordPress <time datetime="...">）。
var datetimeRegex = regexp.MustCompile(`datetime="([0-9T:\-\+\.]+)"`)

// detailCache 详情页链接缓存，降低对源站的压力。
var detailCache sync.Map

type detailCacheEntry struct {
	links     []model.Link
	published time.Time
	expiresAt time.Time
}

// GuohePlugin 果核剥壳插件。
type GuohePlugin struct {
	*plugin.BaseAsyncPlugin
	client *http.Client
}

func init() {
	plugin.RegisterGlobalPlugin(NewGuohePlugin())
	go startDetailCacheCleaner()
}

// NewGuohePlugin 创建插件实例。
func NewGuohePlugin() *GuohePlugin {
	return &GuohePlugin{
		BaseAsyncPlugin: plugin.NewBaseAsyncPlugin(pluginName, defaultPriority),
		client:          newHTTPClient(),
	}
}

// Search 兼容方法。
func (p *GuohePlugin) Search(keyword string, ext map[string]interface{}) ([]model.SearchResult, error) {
	result, err := p.SearchWithResult(keyword, ext)
	if err != nil {
		return nil, err
	}
	return result.Results, nil
}

// SearchWithResult 扩展方法。
func (p *GuohePlugin) SearchWithResult(keyword string, ext map[string]interface{}) (model.PluginSearchResult, error) {
	return p.AsyncSearchWithResult(keyword, p.searchImpl, p.MainCacheKey, ext)
}

// listItem 搜索列表中的单个文章。
type listItem struct {
	title   string
	excerpt string
	date    string
	url     string
}

func (p *GuohePlugin) searchImpl(client *http.Client, keyword string, ext map[string]interface{}) ([]model.SearchResult, error) {
	if p.client != nil {
		client = p.client
	}

	searchKeyword := strings.TrimSpace(keyword)
	if searchKeyword == "" {
		return nil, fmt.Errorf("[%s] 关键词不能为空", p.Name())
	}

	// 1. 搜索列表页
	listURL := fmt.Sprintf("%s/?s=%s", WebsiteURL, url.QueryEscape(searchKeyword))
	items, err := p.fetchList(client, listURL)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("[%s] 未找到相关资源", p.Name())
	}

	// 2. 并发抓详情页
	var (
		wg      sync.WaitGroup
		resultM sync.Mutex
		results []model.SearchResult
		sem     = make(chan struct{}, maxDetailWorkers)
	)

	for _, item := range items {
		item := item
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			links, published := p.fetchDetailLinks(client, item.url)
			if len(links) == 0 {
				return
			}

			content := buildContent(item)
			result := model.SearchResult{
				UniqueID: fmt.Sprintf("%s-%s", p.Name(), hashURL(item.url)),
				Title:    item.title,
				Content:  content,
				Links:    links,
				Tags:     []string{pluginName, "软件"},
				Channel:  "",
				Datetime: published,
			}

			resultM.Lock()
			results = append(results, result)
			resultM.Unlock()
		}()
	}
	wg.Wait()

	if len(results) == 0 {
		return nil, fmt.Errorf("[%s] 未能获取到有效网盘链接", p.Name())
	}

	return plugin.FilterResultsByKeyword(results, searchKeyword), nil
}

// buildContent 组装结果正文：摘要 + 发布日期。
func buildContent(item listItem) string {
	parts := make([]string, 0, 2)
	if excerpt := strings.TrimSpace(item.excerpt); excerpt != "" {
		parts = append(parts, excerpt)
	}
	if date := strings.TrimSpace(item.date); date != "" {
		parts = append(parts, "发布: "+date)
	}
	return strings.Join(parts, "\n")
}

// fetchList 抓取搜索列表页，返回文章条目。
func (p *GuohePlugin) fetchList(client *http.Client, listURL string) ([]listItem, error) {
	body, err := p.getHTML(client, listURL, listURL)
	if err != nil {
		return nil, fmt.Errorf("[%s] 搜索列表页失败: %w", p.Name(), err)
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("[%s] 解析列表页失败: %w", p.Name(), err)
	}

	items := make([]listItem, 0, maxListItems)
	seen := make(map[string]bool)

	doc.Find(".item-content").Each(func(_ int, content *goquery.Selection) {
		if len(items) >= maxListItems {
			return
		}

		a := content.Find("h2 a[href]").First()
		href, ok := a.Attr("href")
		if !ok {
			return
		}
		href = strings.TrimSpace(href)
		if !strings.Contains(href, "ghxi.com/") || !strings.HasSuffix(href, ".html") {
			return
		}
		if seen[href] {
			return
		}
		title := strings.TrimSpace(a.Text())
		if title == "" {
			return
		}

		seen[href] = true
		item := listItem{
			title: title,
			url:   href,
		}
		if excerpt := content.Find(".item-excerpt").First().Text(); excerpt != "" {
			item.excerpt = strings.TrimSpace(excerpt)
		}
		if date := content.Find(".item-meta-li.date").First().Text(); date != "" {
			item.date = strings.TrimSpace(date)
		}
		items = append(items, item)
	})

	return items, nil
}

// fetchDetailLinks 抓取详情页并提取网盘链接（带缓存）。
func (p *GuohePlugin) fetchDetailLinks(client *http.Client, detailURL string) ([]model.Link, time.Time) {
	if cached, ok := detailCache.Load(detailURL); ok {
		if entry, valid := cached.(detailCacheEntry); valid && time.Now().Before(entry.expiresAt) {
			return entry.links, entry.published
		}
		detailCache.Delete(detailURL)
	}

	ctx, cancel := context.WithTimeout(context.Background(), detailTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, detailURL, nil)
	if err != nil {
		return nil, time.Time{}
	}
	setHTMLHeaders(req, detailURL)

	resp, err := doRequestWithRetry(req, client, maxRequestRetries)
	if err != nil {
		return nil, time.Time{}
	}
	defer resp.Body.Close()

	body, err := util.ReadAllLimited(resp.Body, util.MaxUpstreamResponseBytes)
	if err != nil {
		return nil, time.Time{}
	}
	html := string(body)

	links := extractLinks(html)
	var published time.Time
	if m := datetimeRegex.FindStringSubmatch(html); len(m) > 1 {
		if t, perr := time.Parse(time.RFC3339, m[1]); perr == nil {
			published = t
		}
	}

	if len(links) > 0 {
		detailCache.Store(detailURL, detailCacheEntry{
			links:     links,
			published: published,
			expiresAt: time.Now().Add(time.Hour),
		})
	}
	return links, published
}

// extractLinks 从详情页 HTML 提取去重后的网盘链接。
func extractLinks(html string) []model.Link {
	var (
		links []model.Link
		seen  = make(map[string]bool)
	)

	for _, pattern := range linkPatterns {
		for _, loc := range pattern.reg.FindAllString(html, -1) {
			raw := strings.TrimRight(loc, "#")
			if seen[raw] {
				continue
			}
			seen[raw] = true

			links = append(links, model.Link{
				Type:     pattern.typ,
				URL:      raw,
				Password: matchPassword(html, raw),
			})
		}
	}
	return links
}

// matchPassword 提取密码：先看链接 pwd 参数，再找链接附近的提取码文案。
func matchPassword(html, raw string) string {
	if parsed, err := url.Parse(raw); err == nil {
		for _, key := range []string{"pwd", "password", "p", "code"} {
			if value := strings.TrimSpace(parsed.Query().Get(key)); value != "" && len(value) <= 8 {
				return value
			}
		}
	}

	// 找链接在页面中的位置，向后的窗口内找提取码文案
	idx := strings.Index(html, raw)
	if idx >= 0 {
		window := html[idx:min(len(html), idx+300)]
		for _, pattern := range passwordPatterns {
			if matches := pattern.FindStringSubmatch(window); len(matches) > 1 {
				return strings.TrimSpace(matches[1])
			}
		}
	}
	return ""
}

// hashURL 生成结果唯一键：优先取 URL slug，兜底用 FNV 指纹。
func hashURL(raw string) string {
	if parsed, err := url.Parse(raw); err == nil {
		parts := strings.Split(strings.TrimSuffix(parsed.Path, ".html"), "/")
		if len(parts) > 0 && parts[len(parts)-1] != "" {
			return parts[len(parts)-1]
		}
	}
	h := fnv.New32a()
	h.Write([]byte(raw))
	return strconv.FormatUint(uint64(h.Sum32()), 10)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// getHTML 带重试地获取页面文本。
func (p *GuohePlugin) getHTML(client *http.Client, rawURL, referer string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	setHTMLHeaders(req, referer)

	resp, err := doRequestWithRetry(req, client, maxRequestRetries)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := util.ReadAllLimited(resp.Body, util.MaxUpstreamResponseBytes)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// doRequestWithRetry 带指数退避的重试请求。
func doRequestWithRetry(req *http.Request, client *http.Client, maxRetries int) (*http.Response, error) {
	var resp *http.Response
	err := util.DoWithRetry(util.RetryConfig{
		Attempts:   maxRetries,
		BaseDelay:  retryBaseDelay,
		Multiplier: 2,
	}, func(_ int) error {
		r, err := client.Do(req.Clone(req.Context()))
		if err != nil {
			return err
		}
		if r.StatusCode == http.StatusOK {
			resp = r
			return nil
		}
		status := r.StatusCode
		r.Body.Close()
		return fmt.Errorf("HTTP 状态码 %d", status)
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{Renegotiation: tls.RenegotiateFreelyAsClient},
			MaxIdleConns:        httpMaxIdleConns,
			MaxIdleConnsPerHost: httpMaxIdlePerHost,
			MaxConnsPerHost:     httpMaxConnsPerHost,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

func setHTMLHeaders(req *http.Request, referer string) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Referer", referer)
}

func startDetailCacheCleaner() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		detailCache.Range(func(key, value interface{}) bool {
			entry, ok := value.(detailCacheEntry)
			if !ok || now.After(entry.expiresAt) {
				detailCache.Delete(key)
			}
			return true
		})
	}
}
