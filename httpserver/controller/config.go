package controller

import (
	"github.com/bbadbeef/dts_verify_tool/taskmanager"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

// ShowConfig ...
func ShowConfig(c *gin.Context) {
	taskId := c.Query("task_id")
	task := taskmanager.GetTask(taskId)
	if task == nil {
		c.String(http.StatusBadRequest, "task not found")
		return
	}
	c.String(http.StatusOK, task.DisplayConfig())
}

// FlushConfig ...
func FlushConfig(c *gin.Context) {
	taskId := c.Query("task_id")
	task := taskmanager.GetTask(taskId)
	if task == nil {
		c.String(http.StatusBadRequest, "task not found")
		return
	}

	srcConcurrency, _ := strconv.Atoi(c.Query("src_concurrency"))
	dstConcurrency, _ := strconv.Atoi(c.Query("dst_concurrency"))

	task.SetConcurrency(srcConcurrency, dstConcurrency)
	c.String(http.StatusOK, "success")
}

// ShowTaskInfo ...
func ShowTaskInfo(c *gin.Context) {
	taskId := c.Query("task_id")
	task := taskmanager.GetTask(taskId)
	if task == nil {
		c.String(http.StatusBadRequest, "task not found")
		return
	}

	c.String(http.StatusOK, task.Display())
}
