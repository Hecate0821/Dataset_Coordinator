package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"sync"
)

type Task struct {
	Pattern    string `json:"pattern"`
	Status     int    `json:"status"`
	WorkerName string `json:"worker_name"`
}

var tasks []Task
var tasksLock sync.RWMutex

func main() {
	// 初始化任务列表
	loadTasks()

	r := gin.Default()

	// 获取任务
	r.POST("/getTask", getTask)
	// 更新任务状态
	r.POST("/updateTask", updateTask)

	r.Run(":80")
}

func loadTasks() {
	data, err := os.ReadFile("task.json")
	if err != nil {
		fmt.Println("Error reading tasks file:", err)
		return
	}
	err = json.Unmarshal(data, &tasks)
	if err != nil {
		fmt.Println("Error unmarshalling tasks:", err)
	}
}

func getTask(c *gin.Context) {
	tasksLock.Lock()
	defer tasksLock.Unlock()

	for i, task := range tasks {
		if task.Status == 0 {
			task.Status = 1
			task.WorkerName = c.ClientIP() // 使用请求者的IP作为worker名
			tasks[i] = task
			saveTasks()
			c.JSON(http.StatusOK, gin.H{"task": task.Pattern})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "No tasks available"})
}

func updateTask(c *gin.Context) {
	tasksLock.Lock()
	defer tasksLock.Unlock()

	var request struct {
		Task   string `json:"task"`
		Status int    `json:"status"`
	}

	if err := c.BindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	for i, task := range tasks {
		if task.Pattern == request.Task && task.WorkerName == c.ClientIP() && task.Status == 1 {
			task.Status = request.Status
			tasks[i] = task
			saveTasks()
			c.JSON(http.StatusOK, gin.H{"message": "Task updated"})
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"message": "Task not found or not assigned to you"})
}

func saveTasks() {
	data, err := json.Marshal(tasks)
	if err != nil {
		fmt.Println("Error marshalling tasks:", err)
		return
	}
	err = os.WriteFile("task.json", data, 0644)
	if err != nil {
		fmt.Println("Error writing tasks file:", err)
	}
}
