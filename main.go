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
	TaskID       string            `json:"task_id"` // 全局唯一任务ID
	IdempotentID string            `json:"idempotent_id"` // 幂等ID，业务方传入，避免重复投递
	URL          string            `json:"url" binding:"required"`
	Method       string            `json:"method" default:"POST"`
	Headers      map[string]string `json:"headers"`
	Body         interface{}       `json:"body" binding:"required"`
	CallbackURL  string            `json:"callback_url"` // 投递结果回调地址
	Retry        int               `json:"retry" default:"0"` // 当前重试次数
}

// BatchNotificationRequest 批量通知请求
type BatchNotificationRequest struct {
	Notifications []NotificationRequest `json:"notifications" binding:"required,min=1,max=100"`
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
	// 批量提交通知接口
	r.POST("/notify/batch", submitBatchNotificationHandler)
	// 任务状态查询接口
	r.GET("/task/:task_id", getTaskStatusHandler)
	// 死信队列管理接口
	group := r.Group("/dlq")
	{
		group.GET("/list", listDeadLetterHandler)
		group.POST("/retry/:task_id", retryDeadLetterHandler)
		group.POST("/retry/all", retryAllDeadLetterHandler)
		group.DELETE("//:task_id", deleteDeadLetterHandler)
	}

	// 启动worker
	ctx, cancel := context.WithCancel(context.Background())
	go worker(ctx)
	go retryWorker(ctx)
	go callbackWorker(ctx) // 启动回调worker

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

	// 幂等校验
	if req.IdempotentID != "" {
		exists, err := rdb.Exists(ctx, "idempotent:"+req.IdempotentID).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "幂等校验失败"})
			return
		}
		if exists == 1 {
			// 已经处理过，直接返回已存在的TaskID
			taskID, _ := rdb.Get(ctx, "idempotent:"+req.IdempotentID).Result()
			c.JSON(http.StatusOK, gin.H{"status": "success", "task_id": taskID, "message": "通知已提交（幂等命中）"})
			return
		}
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

	// 保存幂等关系
	if req.IdempotentID != "" {
		rdb.SetEX(ctx, "idempotent:"+req.IdempotentID, req.TaskID, 7*24*time.Hour)
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

// submitBatchNotificationHandler 批量提交通知请求
func submitBatchNotificationHandler(c *gin.Context) {
	var req BatchNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	taskIDs := make([]string, 0, len(req.Notifications))
	pipe := rdb.Pipeline()

	for _, notify := range req.Notifications {
		// 幂等校验
		if notify.IdempotentID != "" {
			exists, err := rdb.Exists(ctx, "idempotent:"+notify.IdempotentID).Result()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "幂等校验失败"})
				return
			}
			if exists == 1 {
				// 已经处理过，直接返回已存在的TaskID
				taskID, _ := rdb.Get(ctx, "idempotent:"+notify.IdempotentID).Result()
				taskIDs = append(taskIDs, taskID)
				continue
			}
		}

		// 校验URL
		if err := validateURL(notify.URL); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "URL校验失败: " + err.Error()})
			return
		}

		// 生成TaskID
		notify.TaskID = uuid.NewString()
		if notify.Method == "" {
			notify.Method = "POST"
		}

		// 保存幂等关系
		if notify.IdempotentID != "" {
			pipe.SetEX(ctx, "idempotent:"+notify.IdempotentID, notify.TaskID, 7*24*time.Hour)
		}

		// 保存任务元信息
		taskMeta := map[string]interface{}{
			"url":     notify.URL,
			"method":  notify.Method,
			"status":  "pending",
			"created": time.Now().Unix(),
		}
		taskMetaBytes, _ := json.Marshal(taskMeta)
		pipe.SetEX(ctx, "task:"+notify.TaskID, taskMetaBytes, 7*24*time.Hour)

		// 写入队列
		taskBytes, err := json.Marshal(notify)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "任务序列化失败"})
			return
		}
		pipe.LPush(ctx, "notification_queue", taskBytes)

		taskIDs = append(taskIDs, notify.TaskID)
	}

	// 批量执行Redis操作
	_, err := pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "批量提交失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "task_ids": taskIDs, "count": len(taskIDs), "message": "批量通知已提交"})
}

// listDeadLetterHandler 查询死信队列列表
func listDeadLetterHandler(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}

	start := int64((page - 1) * pageSize)
	end := int64(page*pageSize - 1)

	tasks, err := rdb.LRange(ctx, "dead_letter_queue", start, end).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询死信队列失败"})
		return
	}

	total, err := rdb.LLen(ctx, "dead_letter_queue").Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取总数失败"})
		return
	}

	result := make([]map[string]interface{}, 0, len(tasks))
	for _, taskStr := range tasks {
		var task NotificationRequest
		json.Unmarshal([]byte(taskStr), &task)
		result = append(result, map[string]interface{}{
			"task_id": task.TaskID,
			"url":     task.URL,
			"retry":   task.Retry,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"list":  result,
		"total": total,
		"page":  page,
		"size":  pageSize,
	})
}

// retryDeadLetterHandler 重试单个死信任务
func retryDeadLetterHandler(c *gin.Context) {
	taskID := c.Param("task_id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task_id不能为空"})
		return
	}

	// 遍历死信队列找到对应任务
	tasks, err := rdb.LRange(ctx, "dead_letter_queue", 0, -1).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询死信队列失败"})
		return
	}

	for _, taskStr := range tasks {
		var task NotificationRequest
		json.Unmarshal([]byte(taskStr), &task)
		if task.TaskID == taskID {
			// 从死信队列移除
			rdb.LRem(ctx, "dead_letter_queue", 0, taskStr)
			// 重置重试次数
			task.Retry = 0
			// 写入主队列
			taskBytes, _ := json.Marshal(task)
			rdb.LPush(ctx, "notification_queue", taskBytes)
			// 更新任务状态
			taskMeta := map[string]interface{}{
				"url":    task.URL,
				"method": task.Method,
				"status": "retrying",
				"retry":  0,
			}
			taskMetaBytes, _ := json.Marshal(taskMeta)
			rdb.SetEX(ctx, "task:"+task.TaskID, taskMetaBytes, 7*24*time.Hour)

			c.JSON(http.StatusOK, gin.H{"status": "success", "message": "任务已加入重试队列"})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
}

// retryAllDeadLetterHandler 重试所有死信任务
func retryAllDeadLetterHandler(c *gin.Context) {
	count, err := rdb.LLen(ctx, "dead_letter_queue").Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询死信队列失败"})
		return
	}

	for i := 0; i < int(count); i++ {
		taskStr, err := rdb.RPop(ctx, "dead_letter_queue").Result()
		if err != nil {
			break
		}
		var task NotificationRequest
		json.Unmarshal([]byte(taskStr), &task)
		// 重置重试次数
		task.Retry = 0
		// 写入主队列
		taskBytes, _ := json.Marshal(task)
		rdb.LPush(ctx, "notification_queue", taskBytes)
		// 更新任务状态
		taskMeta := map[string]interface{}{
			"url":    task.URL,
			"method": task.Method,
			"status": "retrying",
			"retry":  0,
		}
		taskMetaBytes, _ := json.Marshal(taskMeta)
		rdb.SetEX(ctx, "task:"+task.TaskID, taskMetaBytes, 7*24*time.Hour)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "count": count, "message": "所有死信任务已加入重试队列"})
}

// deleteDeadLetterHandler 删除死信任务
func deleteDeadLetterHandler(c *gin.Context) {
	taskID := c.Param("task_id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task_id不能为空"})
		return
	}

	// 遍历死信队列找到对应任务
	tasks, err := rdb.LRange(ctx, "dead_letter_queue", 0, -1).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询死信队列失败"})
		return
	}

	for _, taskStr := range tasks {
		var task NotificationRequest
		json.Unmarshal([]byte(taskStr), &task)
		if task.TaskID == taskID {
			// 从死信队列移除
			rdb.LRem(ctx, "dead_letter_queue", 0, taskStr)
			// 更新任务状态为已丢弃
			taskMeta := map[string]interface{}{
				"url":    task.URL,
				"method": task.Method,
				"status": "discarded",
				"retry":  task.Retry,
			}
			taskMetaBytes, _ := json.Marshal(taskMeta)
			rdb.SetEX(ctx, "task:"+task.TaskID, taskMetaBytes, 7*24*time.Hour)

			c.JSON(http.StatusOK, gin.H{"status": "success", "message": "任务已删除"})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
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

			// 投递成功回调
			if task.CallbackURL != "" {
				callbackTask := map[string]interface{}{
					"task_id": task.TaskID,
					"status":  "success",
					"url":     task.URL,
				}
				callbackBytes, _ := json.Marshal(callbackTask)
				rdb.LPush(ctx, "callback_queue", callbackBytes)
			}
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

		// 投递失败回调
		if task.CallbackURL != "" {
			callbackTask := map[string]interface{}{
				"task_id": task.TaskID,
				"status":  "failed",
				"url":     task.URL,
				"error":   "超过最大重试次数",
			}
			callbackBytes, _ := json.Marshal(callbackTask)
			rdb.LPush(ctx, "callback_queue", callbackBytes)
		}
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

// callbackWorker 回调任务执行器
func callbackWorker(ctx context.Context) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("callbackWorker收到停止信号，退出")
			return
		default:
			result, err := rdb.BRPop(ctx, 1*time.Second, "callback_queue").Result()
			if err != nil {
				if err != redis.Nil {
					log.Printf("获取回调任务失败: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var callbackTask map[string]interface{}
			err = json.Unmarshal([]byte(result[1]), &callbackTask)
			if err != nil {
				log.Printf("回调任务解析失败: %v", err)
				continue
			}

			callbackURL := callbackTask["url"].(string)
			callbackBytes, _ := json.Marshal(callbackTask)
			req, err := http.NewRequest("POST", callbackURL, bytes.NewBuffer(callbackBytes))
			if err != nil {
				log.Printf("回调请求创建失败: %v", err)
				continue
			}
			req.Header.Set("Content-Type", "application/json")

			// 回调失败只重试3次
			success := false
			for i := 0; i < 3; i++ {
				resp, err := client.Do(req)
				if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
					resp.Body.Close()
					log.Printf("回调成功: %s", callbackURL)
					success = true
					break
				}
				if resp != nil {
					resp.Body.Close()
				}
				time.Sleep(time.Second * time.Duration(1<<i))
			}
			if !success {
				log.Printf("回调失败超过3次，放弃: %s", callbackURL)
			}
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
