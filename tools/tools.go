package tools

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	hostPrefix = "http://127.0.0.1"
)

// ShowConfig ...
func ShowConfig() {
	url := fmt.Sprintf("%s:%d/show_config", hostPrefix, viper.GetInt("http.port"))
	b, err := getHttp(url)
	if err != nil {
		output(err.Error())
		return
	}
	output(string(b))
}

// ShowTaskInfo ...
func ShowTaskInfo() {
	url := fmt.Sprintf("%s:%d/show_task_info", hostPrefix, viper.GetInt("http.port"))
	b, err := getHttp(url)
	if err != nil {
		output(err.Error())
		return
	}
	output(string(b))
}

// SetConfig ...
func SetConfig(srcConcurrency, dstConcurrency int) {
	var paraStr string
	if srcConcurrency != 0 {
		paraStr += fmt.Sprintf("src_concurrency=%d", srcConcurrency)
	}
	if dstConcurrency != 0 {
		if len(paraStr) != 0 {
			paraStr += "&"
		}
		paraStr += fmt.Sprintf("dst_concurrency=%d", dstConcurrency)
	}
	url := fmt.Sprintf("%s:%d/flush_config?%s", hostPrefix, viper.GetInt("http.port"), paraStr)
	b, err := getHttp(url)
	if err != nil {
		output(err.Error())
		return
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "\t")
	if err != nil {
		output(string(b))
		return
	}
	output(out.String())
}

var (
	client *http.Client
)

func init() {
	client = &http.Client{
		Transport: http.DefaultTransport,
		Timeout:   time.Second * 10,
	}
}

func getHttp(url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func output(str string) {
	os.Stdout.WriteString(str)
}
