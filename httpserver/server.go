package httpserver

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"io"
	"os"
)

// Start ...
func Start() {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Logger())
	engine.Use(gin.Recovery())

	if err := setLogOutput(); err != nil {
		panic(fmt.Sprintf("set gin output error: %s", err.Error()))
	}

	addRouter(engine)

	go func() {
		if err := engine.Run(fmt.Sprintf(":%d", viper.GetInt("http.port"))); err != nil {
			panic(fmt.Sprintf("http server start error: %s", err.Error()))
		}
	}()
}

func setLogOutput() error {
	gin.DisableConsoleColor()
	f, err := os.OpenFile("gin.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		return err
	}
	gin.DefaultWriter = io.MultiWriter(f) //, os.Stdout)
	return nil
}
