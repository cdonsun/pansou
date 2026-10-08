package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"
)

func main() {
	client := &http.Client{
		Timeout: 12 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{Renegotiation: tls.RenegotiateFreelyAsClient},
		},
	}
	req, _ := http.NewRequest("GET", "https://www.dayanzai.me/?s=photoshop", nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Referer", "https://www.dayanzai.me/?s=photoshop")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("ERR:", err)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
	fmt.Println("STATUS:", resp.StatusCode)
	if len(b) > 120 {
		b = b[:120]
	}
	fmt.Println("BODY:", string(b))
}
