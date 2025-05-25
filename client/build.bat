@echo off
echo 正在构建 XMySQL 客户端...

REM 设置环境变量
set GOOS=windows
set GOARCH=amd64

REM 构建客户端
go build -o xmysql-client.exe main.go

if %ERRORLEVEL% EQU 0 (
    echo 构建成功! 可执行文件: xmysql-client.exe
    echo.
    echo 使用方法:
    echo   xmysql-client.exe --help                    显示帮助
    echo   xmysql-client.exe                           使用默认设置连接
    echo   xmysql-client.exe -h 127.0.0.1 -P 3308     指定服务器和端口
    echo   xmysql-client.exe --gui                     启动图形界面
    echo.
) else (
    echo 构建失败!
    exit /b 1
) 