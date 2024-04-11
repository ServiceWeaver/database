package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type Request struct {
	origPort string
	Count    int
	httpReq  []httpReq
}

type metadata struct {
	ReqCount int `json:"ReqCount"`
}

type jsonFormat struct {
	HttpReqs []httpReq `json:"HttpReqs"`
	Metadata metadata  `json:"Metadata"`
}

type httpReq struct {
	Body   url.Values `json:"Body"`
	Method string     `json:"Method"`
	Url    string     `json:"Url"`
}

func NewRequest(reqPath string, origPort string) (*Request, error) {
	jsonData, err := os.ReadFile(reqPath)
	if err != nil {
		return nil, fmt.Errorf("error reading log file, err=%s", err)
	}

	var data jsonFormat
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling json, err=%s", err)
	}

	return &Request{origPort: origPort, Count: data.Metadata.ReqCount, httpReq: data.HttpReqs}, nil
}

func (r *Request) exec(client *http.Client, h *httpReq, port string) (string, error) {
	updatedUrl := strings.ReplaceAll(h.Url, r.origPort, port)
	req, err := http.NewRequest(h.Method, updatedUrl, strings.NewReader(h.Body.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(respBody)
	return bodyString, nil
}
