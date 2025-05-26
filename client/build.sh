#!/bin/bash

echo "正在构建 XMySQL 客户端..."

# 设置环境变量
export GOOS=linux
export GOARCH=amd64

# 构建客户端
go build -o xmysql-client main.go

if [ $? -eq 0 ]; then
    echo "构建成功! 可执行文件: xmysql-client"
    echo ""
    echo "使用方法:"
    echo "  ./xmysql-client --help                    显示帮助"
    echo "  ./xmysql-client                           使用默认设置连接"
    echo "  ./xmysql-client -h 127.0.0.1 -P 3308     指定服务器和端口"
    echo "  ./xmysql-client --gui                     启动图形界面"
    echo ""
    
    # 设置执行权限
    chmod +x xmysql-client
else
    echo "构建失败!"
    exit 1
fi 