package utils

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
)

var node *snowflake.Node

func init() {
	n, err := snowflake.NewNode(1)
	if err != nil {
		panic(err)
	}
	node = n
}

// GenTaskId ...
func GenTaskId() string {
	return node.Generate().String()
}

// InitConfig ...
func InitConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(rootPath)

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			GlobalLogger.Warnf("config file not found")
		} else {
			GlobalLogger.Panicf(fmt.Sprintf("init config error: %s", err.Error()))
		}
	} else {
		viper.WatchConfig()

		viper.OnConfigChange(func(e fsnotify.Event) {
			GlobalLogger.Warnf("config file changed: %s, %s", e.Name, e.String())
		})
	}

	setDefaultConfig()
}

func setDefaultConfig() {
	viper.Set("http.port", 27789)
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
