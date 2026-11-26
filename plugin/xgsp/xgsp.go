package xgsp

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"pansou/model"
	"pansou/plugin"
	"pansou/util"
	"pansou/util/json"
)

const (
	pluginName     = "xgsp"
	apiBase        = "https://api.xgsp.aoch.com:1443/api.php/provide/vod/"
	accessKey      = "02e349c643b429c4a72fd684120c0d7d"
	userAgent      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
	pluginPriority = 3
	pageLimit      = 20
	// 上游关键词搜索较慢（wd 全表 LIKE，生产线实测 20s+），用长超时保证后台
	// 任务能跑完并写入缓存；首个请求由框架 4s 响应窗口先行返回，后续同词命中缓存
	requestTimeout = 70 * time.Second
	clientTimeout  = 65 * time.Second

	// 播放参数随搜索结果一次性带回：取前 N 条作品批量拉详情，失败不影响搜索
	playerTopN          = 5
	playerDetailTimeout = 8 * time.Second
	playerDetailWorkers = 4
)

var cst = time.FixedZone("CST", 8*3600)

func init() {
	plugin.RegisterGlobalPlugin(NewXgspPlugin())
}

// XgspPlugin 西瓜视频（api.xgsp.aoch.com:1443，苹果CMS V10）在线影视插件
type XgspPlugin struct {
	*plugin.BaseAsyncPlugin
}

// NewXgspPlugin 创建 xgsp 插件
func NewXgspPlugin() *XgspPlugin {
	return &XgspPlugin{
		// 搜索由上游接口自行匹配，跳过 Service 层关键词过滤，避免译名/别名被整批丢弃
		BaseAsyncPlugin: plugin.NewBaseAsyncPluginWithFilter(pluginName, pluginPriority, true),
	}
}

// Name 返回插件名称
func (p *XgspPlugin) Name() string {
	return pluginName
}

// DisplayName 返回插件显示名称
func (p *XgspPlugin) DisplayName() string {
	return "西瓜视频"
}

// Description 返回插件描述
func (p *XgspPlugin) Description() string {
	return "在线影视 · api.xgsp.aoch.com:1443（西瓜视频，站内嵌入播放）"
}

// Search 执行搜索并返回结果（兼容性方法）
func (p *XgspPlugin) Search(keyword string, ext map[string]interface{}) ([]model.SearchResult, error) {
	result, err := p.SearchWithResult(keyword, ext)
	if err != nil {
		return nil, err
	}
	return result.Results, nil
}

// SearchWithResult 执行搜索并返回包含 IsFinal 标记的结果
//
// 上游关键词检索 P50 约 5s、最慢 20s，必然赶不上批任务响应窗口：窗口关闭后
// 基类返回"处理中"，由 completeSearchInBackground 用长超时客户端完成抓取并
// 写插件缓存 / 主缓存，同关键词的后续请求即可命中缓存拿到视频结果。
func (p *XgspPlugin) SearchWithResult(keyword string, ext map[string]interface{}) (model.PluginSearchResult, error) {
	return p.AsyncSearchWithResult(keyword, p.searchImpl, p.MainCacheKey, ext)
}

type apiResponse struct {
	Code int       `json:"code"`
	List []vodItem `json:"list"`
}

type vodItem struct {
	ID       int    `json:"vod_id"`
	Name     string `json:"vod_name"`
	TypeName string `json:"type_name"`
	Pic      string `json:"vod_pic"`
	Year     string `json:"vod_year"`
	Area     string `json:"vod_area"`
	Remarks  string `json:"vod_remarks"`
	Time     string `json:"vod_time"`
	Actor    string `json:"vod_actor"`
	Director string `json:"vod_director"`
	PlayFrom string `json:"vod_play_from"`
	PlayURL  string `json:"vod_play_url"`
}

// searchImpl 实际搜索实现
func (p *XgspPlugin) searchImpl(client *http.Client, keyword string, ext map[string]interface{}) ([]model.SearchResult, error) {
	kw := strings.TrimSpace(keyword)
	if kw == "" {
		return nil, nil
	}

	q := url.Values{}
	q.Set("ac", "videolist")
	q.Set("wd", kw)
	q.Set("pg", "1")
	q.Set("limit", strconv.Itoa(pageLimit))
	q.Set("order", "time")
	q.Set("key", accessKey)

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+"?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("[%s] 创建请求失败: %w", pluginName, err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")

	transport := http.DefaultTransport
	if client != nil && client.Transport != nil {
		transport = client.Transport
	}
	slowClient := &http.Client{Timeout: clientTimeout, Transport: transport}

	resp, err := slowClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("[%s] 搜索请求失败: %w", pluginName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("[%s] 搜索响应状态码异常: %d", pluginName, resp.StatusCode)
	}

	body, err := util.ReadAllLimited(resp.Body, util.MaxUpstreamResponseBytes)
	if err != nil {
		return nil, fmt.Errorf("[%s] 读取响应失败: %w", pluginName, err)
	}

	var apiResp apiResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("[%s] JSON 解析失败: %w", pluginName, err)
	}
	if apiResp.Code != 1 {
		return nil, fmt.Errorf("[%s] API 返回异常码: %d", pluginName, apiResp.Code)
	}

	results := convertResults(apiResp.List)
	enrichPlayers(transport, results)
	return results, nil
}

// enrichPlayers 为前 N 条结果补全播放参数（线路/选集）
// 只做一次批量详情请求；失败或上游不可达时保持 player 为空，不影响搜索结果
func enrichPlayers(transport http.RoundTripper, results []model.SearchResult) {
	ids := make([]int, 0, playerTopN)
	for i := range results {
		if len(ids) >= playerTopN {
			break
		}
		if id, err := strconv.Atoi(strings.TrimPrefix(results[i].UniqueID, pluginName+"-")); err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}

	players := fetchPlayers(transport, ids)
	if len(players) == 0 {
		return
	}
	for i := range results {
		id, err := strconv.Atoi(strings.TrimPrefix(results[i].UniqueID, pluginName+"-"))
		if err != nil || id <= 0 {
			continue
		}
		if pl, ok := players[id]; ok && len(results[i].Links) > 0 {
			results[i].Links[0].Player = pl
		}
	}
}

// fetchPlayers 批量取详情，批量失败后逐个兜底（并发 ≤ playerDetailWorkers）
func fetchPlayers(transport http.RoundTripper, ids []int) map[int]*model.LinkPlayer {
	if out := fetchPlayerBatch(transport, ids); len(out) > 0 {
		return out
	}
	out := make(map[int]*model.LinkPlayer, len(ids))
	sem := make(chan struct{}, playerDetailWorkers)
	mu := sync.Mutex{}
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if pl := fetchPlayerBatch(transport, []int{id}); len(pl) > 0 {
				mu.Lock()
				out[id] = pl[id]
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	return out
}

// fetchPlayerBatch 请求 ac=detail&ids=...，单次超时 8s
func fetchPlayerBatch(transport http.RoundTripper, ids []int) map[int]*model.LinkPlayer {
	strIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		strIDs = append(strIDs, strconv.Itoa(id))
	}
	q := url.Values{}
	q.Set("ac", "detail")
	q.Set("ids", strings.Join(strIDs, ","))
	q.Set("key", accessKey)

	ctx, cancel := context.WithTimeout(context.Background(), playerDetailTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+"?"+q.Encode(), nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json, text/plain, */*")

	client := &http.Client{Timeout: playerDetailTimeout, Transport: transport}
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, err := util.ReadAllLimited(resp.Body, util.MaxUpstreamResponseBytes)
	if err != nil {
		return nil
	}
	var apiResp apiResponse
	if err := json.Unmarshal(body, &apiResp); err != nil || apiResp.Code != 1 {
		return nil
	}

	out := make(map[int]*model.LinkPlayer, len(apiResp.List))
	for _, item := range apiResp.List {
		if pl := buildPlayer(item); pl != nil {
			out[pl.ID] = pl
		}
	}
	return out
}

// buildPlayer 解析苹果CMS 的 vod_play_from / vod_play_url 为播放参数
func buildPlayer(item vodItem) *model.LinkPlayer {
	if item.ID <= 0 {
		return nil
	}
	sources := parsePlaySources(item.PlayFrom, item.PlayURL)
	if len(sources) == 0 {
		return nil
	}
	return &model.LinkPlayer{
		ID:        item.ID,
		Title:     strings.TrimSpace(item.Name),
		Poster:    playerHTTPURL(item.Pic),
		UpdatedAt: strings.TrimSpace(item.Time),
		Sources:   sources,
	}
}

func parsePlaySources(fromRaw, urlRaw string) []model.LinkPlayerSource {
	fromList := strings.Split(fromRaw, "$$$")
	urlList := strings.Split(urlRaw, "$$$")
	sources := make([]model.LinkPlayerSource, 0, len(urlList))

	for i, block := range urlList {
		if len(sources) >= 20 {
			break
		}
		flag := ""
		if i < len(fromList) {
			flag = strings.TrimSpace(fromList[i])
		}
		if flag == "" {
			flag = fmt.Sprintf("线路%d", i+1)
		}

		episodes := make([]model.LinkPlayerEpisode, 0, 8)
		for _, part := range strings.Split(block, "#") {
			if len(episodes) >= 500 {
				break
			}
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			name, link := "正片", part
			if pos := strings.Index(part, "$"); pos >= 0 {
				name = strings.TrimSpace(part[:pos])
				link = strings.TrimSpace(part[pos+1:])
			}
			link = playerHTTPURL(link)
			if link == "" {
				continue
			}
			if name == "" {
				name = fmt.Sprintf("第%d集", len(episodes)+1)
			}
			episodes = append(episodes, model.LinkPlayerEpisode{
				Name: name,
				URL:  link,
				Kind: playerLinkKind(link),
			})
		}
		if len(episodes) > 0 {
			sources = append(sources, model.LinkPlayerSource{Flag: flag, Episodes: episodes})
		}
	}
	return sources
}

func playerHTTPURL(v string) string {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") {
		return v
	}
	return ""
}

func playerLinkKind(u string) string {
	lu := strings.ToLower(u)
	if i := strings.IndexAny(lu, "?#"); i >= 0 {
		lu = lu[:i]
	}
	switch {
	case strings.HasSuffix(lu, ".m3u8"):
		return "hls"
	case strings.HasSuffix(lu, ".mp4"), strings.HasSuffix(lu, ".m4v"),
		strings.HasSuffix(lu, ".webm"), strings.HasSuffix(lu, ".ogv"):
		return "video"
	}
	return "page"
}

func convertResults(items []vodItem) []model.SearchResult {
	results := make([]model.SearchResult, 0, len(items))
	for _, item := range items {
		result, ok := convertResult(item)
		if !ok {
			continue
		}
		results = append(results, result)
	}
	return results
}

func convertResult(item vodItem) (model.SearchResult, bool) {
	id := item.ID
	title := strings.TrimSpace(item.Name)
	if id <= 0 || title == "" {
		return model.SearchResult{}, false
	}

	// 链接仅作为作品标识 + 详情数据出口；前端识别 ids 后改用站内 /video.php 嵌入播放
	linkURL := fmt.Sprintf("%s?ac=videolist&ids=%d&key=%s", apiBase, id, accessKey)

	images := []string{}
	if pic := strings.TrimSpace(item.Pic); strings.HasPrefix(pic, "http://") || strings.HasPrefix(pic, "https://") {
		images = append(images, pic)
	}

	return model.SearchResult{
		UniqueID: fmt.Sprintf("%s-%d", pluginName, id),
		Title:    title,
		Content:  buildContent(item),
		Datetime: parseVodTime(item.Time),
		Links: []model.Link{
			{
				Type:      "video",
				URL:       linkURL,
				WorkTitle: title,
			},
		},
		Images: images,
	}, true
}

func buildContent(item vodItem) string {
	parts := make([]string, 0, 5)
	for _, v := range []string{item.TypeName, item.Year, item.Area, item.Remarks} {
		if s := strings.TrimSpace(v); s != "" {
			parts = append(parts, s)
		}
	}
	if s := strings.TrimSpace(item.Actor); s != "" {
		parts = append(parts, "主演："+s)
	}
	return strings.Join(parts, " · ")
}

func parseVodTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, raw, cst); err == nil {
			return t
		}
	}
	return time.Time{}
}
