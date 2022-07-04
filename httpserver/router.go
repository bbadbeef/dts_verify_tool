package httpserver

import (
	"github.com/bbadbeef/dts_verify_tool/httpserver/controller"
	"github.com/gin-gonic/gin"
)

func addRouter(engine *gin.Engine) {
	engine.GET("show_config", controller.ShowConfig)
	engine.GET("flush_config", controller.FlushConfig)
	engine.GET("show_task_info", controller.ShowTaskInfo)
}
