package httpserver

import (
	"github.com/gin-gonic/gin"
	"mongo_compare/httpserver/controller"
)

func addRouter(engine *gin.Engine) {
	engine.GET("show_config", controller.ShowConfig)
	engine.GET("flush_config", controller.FlushConfig)
	engine.GET("show_task_info", controller.ShowTaskInfo)
}
