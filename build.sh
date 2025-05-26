#!/bin/bash -

set -eu

echo "========================================"
echo "Building xmysql-server and client"
echo "========================================"

# 创建 dist 目录结构
mkdir -p dist/conf

# 获取 Git 信息
VERSION=$(git describe --abbrev=0 --tags 2>/dev/null || echo "v1.0.0")
REVCNT=$(git rev-list --count HEAD 2>/dev/null || echo "0")
DEVCNT=$(git rev-list --count $VERSION 2>/dev/null || echo "0")

if test $REVCNT != $DEVCNT
then
	VERSION="$VERSION.dev$(expr $REVCNT - $DEVCNT)"
fi
echo "Version: $VERSION"

GITCOMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILDTIME=$(date -u +%Y/%m/%d-%H:%M:%S)

LDFLAGS="-X main.VERSION=$VERSION -X main.BUILDTIME=$BUILDTIME -X main.GITCOMMIT=$GITCOMMIT"
if [[ -n "${EX_LDFLAGS:-""}" ]]
then
	LDFLAGS="$LDFLAGS $EX_LDFLAGS"
fi

buildServer(){
	echo "Building xmysql-server for $1 $2..."
	GOOS=$1 GOARCH=$2 go build \
	  -o dist/$3 \
		-tags vfs \
		-ldflags "$LDFLAGS" \
		-gcflags "all=-N -l" \
		main.go
	
	if [ $? -eq 0 ]; then
		echo "Successfully built: dist/$3"
	else
		echo "Error: Failed to build xmysql-server"
		exit 1
	fi
}

buildClient() {
	echo "Building xmysql-client for $1 $2..."
	GOOS=$1 GOARCH=$2 go build \
	  -o dist/$3 \
		-tags vfs \
		-ldflags "$LDFLAGS" \
		-gcflags "all=-N -l" \
		client/main.go
	
	if [ $? -eq 0 ]; then
		echo "Successfully built: dist/$3"
	else
		echo "Error: Failed to build xmysql-client"
		exit 1
	fi
}

copyConfigs() {
	echo "Copying configuration files from conf/ to dist/conf/..."
	
	if cp conf/my.ini dist/conf/ 2>/dev/null; then
		echo "Copied: my.ini"
	else
		echo "Warning: Failed to copy my.ini"
	fi
	
	if cp conf/default.ini dist/conf/ 2>/dev/null; then
		echo "Copied: default.ini"
	else
		echo "Warning: Failed to copy default.ini"
	fi
	
	if cp conf/log.xml dist/conf/ 2>/dev/null; then
		echo "Copied: log.xml"
	else
		echo "Warning: Failed to copy log.xml"
	fi
	
	if cp conf/config.yml dist/conf/ 2>/dev/null; then
		echo "Copied: config.yml"
	else
		echo "Warning: Failed to copy config.yml"
	fi
}

echo "========================================"
echo "Building xmysql-server..."
echo "========================================"

# 根据操作系统选择合适的可执行文件扩展名
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    # Windows环境
    buildServer windows amd64 xmysql-server.exe
    echo "========================================"
    echo "Building xmysql-client..."
    echo "========================================"
    buildClient windows amd64 xmysql-client.exe
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS环境
    buildServer darwin amd64 xmysql-server
    echo "========================================"
    echo "Building xmysql-client..."
    echo "========================================"
    buildClient darwin amd64 xmysql-client
else
    # Linux环境
    buildServer linux amd64 xmysql-server
    echo "========================================"
    echo "Building xmysql-client..."
    echo "========================================"
    buildClient linux amd64 xmysql-client
fi

echo "========================================"
echo "Copying configuration files..."
echo "========================================"
copyConfigs

echo "========================================"
echo "Build completed successfully!"
echo "========================================"
echo "Output directory: dist/"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    echo "- xmysql-server.exe"
    echo "- xmysql-client.exe"
else
    echo "- xmysql-server"
    echo "- xmysql-client"
fi
echo "- conf/my.ini"
echo "- conf/default.ini"
echo "- conf/log.xml"
echo "- conf/config.yml"
echo "========================================"