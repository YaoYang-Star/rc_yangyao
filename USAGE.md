# 使用说明

## 快速开始

### 1. 环境依赖
- Redis 5.0+
- Go 1.21+

### 2. 启动服务
```bash
# 安装依赖
go mod download

# 启动服务
go run main.go
```

### 3. 提交通知请求
```bash
curl -X POST http://localhost:8080/notify \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/webhook",
    "method": "POST",
    "headers": {
      "X-API-Key": "your-secret-key"
    },
    "body": {
      "event": "user_register",
      "user_id": 12345,
      "timestamp": 1712345678
    }
  }'
```

### 4. 查看死信队列
```bash
redis-cli LRANGE dead_letter_queue 0 -1
```

## 核心配置
- 最大重试次数：24次
- 最大重试间隔：1小时
- HTTP请求超时：10秒
- 队列名称：
  - 主队列：notification_queue
  - 重试队列：retry_queue
  - 死信队列：dead_letter_queue
