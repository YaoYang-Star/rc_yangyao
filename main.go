package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

// NotificationRequest 业务系统提交的通知请求
type NotificationRequest struct {
	URL     string            `json:"url" binding:"required"`
	Method  string            `json:"method" default:"POST"`
	Headers map[string]string `json:"headers"`
	Body    interface{}       `json:"body" binding:"required"`
	Retry   int               `json:"retry" default:"0"` // 当前重试次数
}

func init() {
	// 初始化Redis连接
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}

func main() {
	r := gin.Default()

	// 业务系统提交通知的接口
	r.POST("/notify", submitNotificationHandler)

	// 启动投递worker
	go worker()
	// 启动重试调度worker
	go retryWorker()

	log.Println("API通知服务启动，端口:8080")
	r.Run(":8080")
}

// submitNotificationHandler 接收业务系统的通知请求
func submitNotificationHandler(c *gin.Context) {
	var req NotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 序列化后写入Redis队列
	taskBytes, _ := json.Marshal(req)
	err := rdb.LPush(ctx, "notification_queue", taskBytes).Err()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "提交失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "通知已提交"})
}

// worker 投递任务执行器
func worker() {
	for {
		// 从队列中取出任务
		result, err := rdb.BRPop(ctx, 0, "notification_queue").Result()
		if err != nil {
			log.Printf("获取任务失败: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var task NotificationRequest
		err = json.Unmarshal([]byte(result[1]), &task)
		if err != nil {
			log.Printf("任务解析失败: %v", err)
			continue
		}

		// 执行投递
		err = deliverNotification(task)
		if err != nil {
			log.Printf("投递失败: %v, 当前重试次数: %d", err, task.Retry)
			handleFailedTask(task)
			continue
		}

		log.Printf("投递成功: %s", task.URL)
	}
}

// deliverNotification 实际执行HTTP投递
func deliverNotification(task NotificationRequest) error {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	bodyBytes, err := json.Marshal(task.Body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(task.Method, task.URL, nil)
	if err != nil {
		return err
	}

	// 设置Header
	for k, v := range task.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 2xx状态码认为成功
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	return &DeliveryError{StatusCode: resp.StatusCode}
}

// handleFailedTask 处理失败的任务，进入重试队列
func handleFailedTask(task NotificationRequest) {
	task.Retry++
	if task.Retry > 24 {
		// 超过最大重试次数，进入死信队列
		taskBytes, _ := json.Marshal(task)
		rdb.LPush(ctx, "dead_letter_queue", taskBytes)
		log.Printf("任务进入死信队列: %s", task.URL)
		return
	}

	// 指数退避，计算延迟时间
	delay := time.Second * time.Duration(1<<task.Retry)
	if delay > time.Hour {
		delay = time.Hour
	}

	// 写入延迟队列
	taskBytes, _ := json.Marshal(task)
	rdb.ZAdd(ctx, "retry_queue", &redis.Z{
		Score:  float64(time.Now().Add(delay).Unix()),
		Member: taskBytes,
	})
}

// retryWorker 重试任务调度器
func retryWorker() {
	for {
		now := time.Now().Unix()
		// 取出已经到了执行时间的重试任务
		result, err := rdb.ZRangeByScore(ctx, "retry_queue", &redis.ZRangeBy{
			Min:    "0",
			Max:    string(rune(now)),
			Count:  100,
			Offset: 0,
		}).Result()
		if err != nil {
			log.Printf("获取重试任务失败: %v", err)
			time.Sleep(time.Second)
			continue
		}

		for _, taskStr := range result {
			// 从重试队列删除
			rdb.ZRem(ctx, "retry_queue", taskStr)
			// 加入主队列执行
			rdb.LPush(ctx, "notification_queue", taskStr)
		}

		time.Sleep(time.Second)
	}
}

// DeliveryError 投递错误
type DeliveryError struct {
	StatusCode int
}

func (e *DeliveryError) Error() string {
	return "投递失败，状态码: " + string(rune(e.StatusCode))
}
