package utils

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var node *snowflake.Node

const (
	baseTs = 1660102554000
)

func init() {
	rand.Seed(time.Now().Unix())
}

// GenTaskId ...
func GenTaskId() string {
	return fmt.Sprintf("%d", (time.Now().UnixMilli()-baseTs)*1000+int64(rand.Intn(1000)))
}

func init() {
	n, err := snowflake.NewNode(1)
	if err != nil {
		panic(err)
	}
	node = n
}

// GenTaskId ...
//func GenTaskId() string {
//	return node.Generate().String()
//}

// InitConfig ...
func InitConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(rootPath)

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			fmt.Println("config file not found")
		} else {
			panic(fmt.Sprintf("init config error: %s", err.Error()))
		}
	} else {
		viper.WatchConfig()

		viper.OnConfigChange(func(e fsnotify.Event) {
			fmt.Printf("config file changed: %s, %s\n", e.Name, e.String())
		})
	}
}

// SetDefaultConfig ...
func SetDefaultConfig() {
	viper.Set("http.port", 27789)
	viper.Set("log.file", logPath)
}

var (
	rootPath string
)

func init() {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	rootPath = filepath.Dir(ex)

	err = os.Chdir(rootPath)
	if err != nil {
		panic(err)
	}
}

var (
	exitFunc []func()
)

// RegisterExitFunc ...
func RegisterExitFunc(f func()) {
	exitFunc = append(exitFunc, f)
}

// GracefullyExit ...
func GracefullyExit() {
	for _, f := range exitFunc {
		if f != nil {
			f()
		}
	}
	os.Exit(0)
}
