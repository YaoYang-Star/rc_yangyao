package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

// Config 服务配置
type Config struct {
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	HTTPPort       string
	MaxRetry       int
	RequestTimeout time.Duration
	APIKey         string
}

// NotificationRequest 业务系统提交的通知请求
type NotificationRequest struct {
	TaskID  string            `json:"task_id"` // 全局唯一任务ID
	URL     string            `json:"url" binding:"required"`
	Method  string            `json:"method" default:"POST"`
	Headers map[string]string `json:"headers"`
	Body    interface{}       `json:"body" binding:"required"`
	Retry   int               `json:"retry" default:"0"` // 当前重试次数
}

// 全局配置
var config = Config{
	RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
	RedisPassword:  getEnv("REDIS_PASSWORD", ""),
	RedisDB:        getEnvInt("REDIS_DB", 0),
	HTTPPort:       getEnv("HTTP_PORT", ":8080"),
	MaxRetry:       getEnvInt("MAX_RETRY", 24),
	RequestTimeout: time.Duration(getEnvInt("REQUEST_TIMEOUT", 10)) * time.Second,
	APIKey:         getEnv("API_KEY", ""),
}

func init() {
	// 初始化Redis连接
	rdb = redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})
}

func main() {
	// 设置gin模式
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// 健康检查接口
	r.GET("/health", healthHandler)

	// API鉴权中间件
	r.Use(authMiddleware())

	// 业务系统提交通知的接口
	r.POST("/notify", submitNotificationHandler)
	// 任务状态查询接口
	r.GET("/task/:task_id", getTaskStatusHandler)

	// 启动worker
	ctx, cancel := context.WithCancel(context.Background())
	go worker(ctx)
	go retryWorker(ctx)

	// 优雅停机处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动HTTP服务
	srv := &http.Server{
		Addr:    config.HTTPPort,
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("服务启动失败: %v", err)
		}
	}()

	log.Printf("API通知服务启动成功，端口: %s", config.HTTPPort)

	// 等待停机信号
	<-sigChan
	log.Println("收到停机信号，开始优雅停机...")

	// 停止接收新请求，等待现有任务处理完成
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("服务强制关闭: %v", err)
	}

	log.Println("服务已安全停止")
}

// authMiddleware API鉴权中间件
func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 没有配置API Key时跳过鉴权
		if config.APIKey == "" {
			c.Next()
			return
		}

		reqAPIKey := c.GetHeader("X-API-Key")
		if reqAPIKey == "" || reqAPIKey != config.APIKey {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "无效的API Key"})
			return
		}

		c.Next()
	}
}

// healthHandler 健康检查接口
func healthHandler(c *gin.Context) {
	// 检查Redis连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": "Redis连接失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

// submitNotificationHandler 接收业务系统的通知请求
func submitNotificationHandler(c *gin.Context) {
	var req NotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 校验URL，防止SSRF攻击
	if err := validateURL(req.URL); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 生成全局唯一任务ID
	req.TaskID = uuid.NewString()
	if req.Method == "" {
		req.Method = "POST"
	}

	// 保存任务元信息
	taskMeta := map[string]interface{}{
		"url":     req.URL,
		"method":  req.Method,
		"status":  "pending",
		"created": time.Now().Unix(),
	}
	taskMetaBytes, _ := json.Marshal(taskMeta)
	rdb.SetEX(ctx, "task:"+req.TaskID, taskMetaBytes, 7*24*time.Hour)

	// 序列化后写入Redis队列
	taskBytes, err := json.Marshal(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "任务序列化失败"})
		return
	}
	err = rdb.LPush(ctx, "notification_queue", taskBytes).Err()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "提交失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "task_id": req.TaskID, "message": "通知已提交"})
}

// getTaskStatusHandler 查询任务状态
func getTaskStatusHandler(c *gin.Context) {
	taskID := c.Param("task_id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task_id不能为空"})
		return
	}

	taskMetaStr, err := rdb.Get(ctx, "task:"+taskID).Result()
	if err == redis.Nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	var taskMeta map[string]interface{}
	json.Unmarshal([]byte(taskMetaStr), &taskMeta)
	c.JSON(http.StatusOK, gin.H{"task_id": taskID, "meta": taskMeta})
}

// validateURL 校验URL安全，防止SSRF攻击
func validateURL(urlStr string) error {
	// 解析URL
	host, _, err := net.SplitHostPort(strings.TrimPrefix(urlStr, "http://"))
	if err != nil {
		host, _, err = net.SplitHostPort(strings.TrimPrefix(urlStr, "https://"))
		if err != nil {
			host = strings.TrimPrefix(strings.TrimPrefix(urlStr, "http://"), "https://")
			if idx := strings.Index(host, "/"); idx != -1 {
				host = host[:idx]
			}
		}
	}

	// 解析IP
	ip, err := net.ResolveIPAddr("ip", host)
	if err != nil {
		return nil // 域名解析失败的情况交给投递时处理
	}

	// 禁止私有IP和内网地址
	ipAddr := ip.IP
	if ipAddr.IsPrivate() || ipAddr.IsLoopback() || ipAddr.IsLinkLocalUnicast() || ipAddr.IsLinkLocalMulticast() {
		return &SSRFError{Message: "禁止请求内网地址"}
	}

	return nil
}

// worker 投递任务执行器
func worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("worker收到停止信号，退出")
			return
		default:
			// 从队列中取出任务，超时1秒避免阻塞
			result, err := rdb.BRPop(ctx, 1*time.Second, "notification_queue").Result()
			if err != nil {
				if err != redis.Nil {
					log.Printf("获取任务失败: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var task NotificationRequest
			err = json.Unmarshal([]byte(result[1]), &task)
			if err != nil {
				log.Printf("任务解析失败: %v", err)
				continue
			}

			// 更新任务状态为处理中
			taskMeta := map[string]interface{}{
				"url":    task.URL,
				"method": task.Method,
				"status": "processing",
				"retry":  task.Retry,
			}
			taskMetaBytes, _ := json.Marshal(taskMeta)
			rdb.SetEX(ctx, "task:"+task.TaskID, taskMetaBytes, 7*24*time.Hour)

			// 执行投递
			err = deliverNotification(task)
			if err != nil {
				log.Printf("[任务 %s] 投递失败: %v, 当前重试次数: %d", task.TaskID, err, task.Retry)
				handleFailedTask(task)
				continue
			}

			// 更新任务状态为成功
			taskMeta["status"] = "success"
			taskMeta["finished"] = time.Now().Unix()
			taskMetaBytes, _ = json.Marshal(taskMeta)
			rdb.SetEX(ctx, "task:"+task.TaskID, taskMetaBytes, 7*24*time.Hour)

			log.Printf("[任务 %s] 投递成功: %s", task.TaskID, task.URL)
		}
	}
}

// deliverNotification 实际执行HTTP投递
func deliverNotification(task NotificationRequest) error {
	client := &http.Client{
		Timeout: config.RequestTimeout,
	}

	bodyBytes, err := json.Marshal(task.Body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(task.Method, task.URL, bytes.NewBuffer(bodyBytes))
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
	if task.Retry > config.MaxRetry {
		// 超过最大重试次数，进入死信队列
		taskBytes, _ := json.Marshal(task)
		rdb.LPush(ctx, "dead_letter_queue", taskBytes)
		// 更新任务状态为失败
		taskMeta := map[string]interface{}{
			"url":    task.URL,
			"method": task.Method,
			"status": "failed",
			"retry":  task.Retry,
			"error":  "超过最大重试次数，进入死信队列",
		}
		taskMetaBytes, _ := json.Marshal(taskMeta)
		rdb.SetEX(ctx, "task:"+task.TaskID, taskMetaBytes, 7*24*time.Hour)
		log.Printf("[任务 %s] 进入死信队列: %s", task.TaskID, task.URL)
		return
	}

	// 更新任务状态为重试中
	taskMeta := map[string]interface{}{
		"url":    task.URL,
		"method": task.Method,
		"status": "retrying",
		"retry":  task.Retry,
	}
	taskMetaBytes, _ := json.Marshal(taskMeta)
	rdb.SetEX(ctx, "task:"+task.TaskID, taskMetaBytes, 7*24*time.Hour)

	// 指数退避，计算延迟时间
	delay := time.Second * time.Duration(1<<task.Retry)
	if delay > time.Hour {
		delay = time.Hour
	}

	// 写入延迟队列
	taskBytes, err := json.Marshal(task)
	if err != nil {
		log.Printf("[任务 %s] 序列化失败: %v", task.TaskID, err)
		return
	}
	_, err = rdb.ZAdd(ctx, "retry_queue", &redis.Z{
		Score:  float64(time.Now().Add(delay).Unix()),
		Member: taskBytes,
	}).Result()
	if err != nil {
		log.Printf("[任务 %s] 写入重试队列失败: %v", task.TaskID, err)
	}
}

// retryWorker 重试任务调度器
func retryWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("retryWorker收到停止信号，退出")
			return
		default:
			now := time.Now().Unix()
			// 取出已经到了执行时间的重试任务
			result, err := rdb.ZRangeByScore(ctx, "retry_queue", &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.FormatInt(now, 10),
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
				_, err := rdb.ZRem(ctx, "retry_queue", taskStr).Result()
				if err != nil {
					log.Printf("删除重试任务失败: %v", err)
					continue
				}
				// 加入主队列执行
				_, err = rdb.LPush(ctx, "notification_queue", taskStr).Result()
				if err != nil {
					log.Printf("加入主队列失败: %v", err)
					// 失败则放回重试队列
					rdb.ZAdd(ctx, "retry_queue", &redis.Z{
						Score:  float64(now + 10), // 10秒后重试
						Member: taskStr,
					})
				}
			}

			time.Sleep(time.Second)
		}
	}
}

// DeliveryError 投递错误
type DeliveryError struct {
	StatusCode int
}

func (e *DeliveryError) Error() string {
	return "投递失败，状态码: " + strconv.Itoa(e.StatusCode)
}

// SSRFError SSRF防护错误
type SSRFError struct {
	Message string
}

func (e *SSRFError) Error() string {
	return e.Message
}

// getEnv 获取环境变量，默认值
func getEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

// getEnvInt 获取整数类型环境变量，默认值
func getEnvInt(key string, defaultValue int) int {
	if value, ok := os.LookupEnv(key); ok {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
