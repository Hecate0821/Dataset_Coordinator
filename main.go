package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"sync"
	"time"
)

type Task struct {
	Pattern      string `json:"pattern"`
	Status       int    `json:"status"`
	WorkerName   string `json:"worker_name"`
	AssignedTime string `json:"assigned_time"`
	FinishedTime string `json:"finished_time"`
	ExecuteCount int    `json:"execute_count"`
}

var tasks []Task
var tasksLock sync.RWMutex

const FINISHED int = 2
const PROCESSING int = 1
const UNFINISHED int = 0

const WarningExecutedCount int = 5

const TaskResetInterval = 3 * time.Hour

func main() {
	// 初始化日志
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.InfoLevel)

	// 初始化任务列表
	loadTasks()

	// 设置定时任务
	ticker := time.NewTicker(1 * time.Hour) // 每小时检查一次
	go func() {
		for range ticker.C {
			resetOldTasks()
		}
	}()

	r := gin.Default()

	r.POST("/getTask", logRequest(getTask))
	r.POST("/completeTask", logRequest(completeTask))
	r.POST("/withdrawTask", logRequest(withdrawTask))

	err := r.Run(":80")
	if err != nil {
		return
	}
}

func resetOldTasks() {
	tasksLock.Lock()
	defer tasksLock.Unlock()

	now := time.Now().In(time.FixedZone("CST", 8*3600))
	for i, task := range tasks {
		if task.Status == PROCESSING {
			assignedTime, err := time.Parse("2006-01-02 15:04:05", task.AssignedTime)
			if err == nil && now.Sub(assignedTime) > TaskResetInterval {
				task.Status = UNFINISHED
				task.AssignedTime = ""
				task.WorkerName = ""
				tasks[i] = task
			}
		}
	}
	saveTasks()
}

func loadTasks() {
	data, err := os.ReadFile("task.json")
	if err != nil {
		logrus.WithError(err).Error("Error reading tasks file")
		return
	}
	err = json.Unmarshal(data, &tasks)
	if err != nil {
		logrus.WithError(err).Error("Error unmarshalling tasks")
	}
}

func getTask(c *gin.Context) {
	tasksLock.Lock()
	defer tasksLock.Unlock()

	var request struct {
		WorkerName   string `json:"worker_name"`
		ExecuteCount int    `json:"execute_count"`
	}

	if err := c.BindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if request.ExecuteCount > WarningExecutedCount {
		logrus.WithFields(logrus.Fields{
			"worker_name":   request.WorkerName,
			"execute_count": request.ExecuteCount,
		}).Warn("Exceeded maximum number of execute count")
	}

	for i, task := range tasks {
		if task.Status == UNFINISHED {
			task.Status = PROCESSING
			task.WorkerName = request.WorkerName
			task.ExecuteCount = task.ExecuteCount + 1
			task.AssignedTime = time.Now().In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05")
			tasks[i] = task
			saveTasks()
			c.JSON(http.StatusOK, gin.H{"task": task.Pattern})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "No tasks available"})
}

func completeTask(c *gin.Context) {
	tasksLock.Lock()
	defer tasksLock.Unlock()

	var request struct {
		Task       string `json:"task"`
		WorkerName string `json:"worker_name"`
	}

	if err := c.BindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	for i, task := range tasks {
		if task.Pattern == request.Task && task.WorkerName == request.WorkerName && task.Status == PROCESSING {
			task.Status = FINISHED
			task.FinishedTime = time.Now().In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05")
			tasks[i] = task
			saveTasks()
			c.JSON(http.StatusOK, gin.H{"message": "Task updated"})
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"message": "Task not found or not assigned to you"})
}

func withdrawTask(c *gin.Context) {
	tasksLock.Lock()
	defer tasksLock.Unlock()

	var request struct {
		WorkerName string `json:"worker_name"`
	}

	if err := c.BindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	for i, task := range tasks {
		if task.Status == PROCESSING && task.WorkerName == request.WorkerName {
			task.Status = UNFINISHED
			task.FinishedTime = ""
			task.AssignedTime = ""
			task.WorkerName = ""
			tasks[i] = task
			break
		}

		if i == len(tasks)-1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Task not found or not assigned to you"})
			return
		}
	}

	saveTasks()
	c.JSON(http.StatusOK, gin.H{"message": "Task withdrawn"})
	return
}

func saveTasks() {
	data, err := json.Marshal(tasks)
	if err != nil {
		logrus.WithError(err).Error("Error marshalling tasks")
		return
	}
	err = os.WriteFile("task.json", data, 0644)
	if err != nil {
		logrus.WithError(err).Error("Error writing tasks file")
	}
}

func logRequest(handlerFunc gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		logrus.WithFields(logrus.Fields{
			"method": c.Request.Method,
			"path":   c.Request.URL.Path,
			"ip":     c.ClientIP(),
		}).Info("Received request")
		handlerFunc(c)
	}
}
