# XMySQL 客户端

这是一个类似 MySQL 官方客户端的程序，可以连接 XMySQL 服务器并执行 SQL 命令。

## 功能特性

- 🔗 **连接管理**: 支持连接到 XMySQL 服务器
- 💻 **命令行界面**: 类似 MySQL 官方客户端的交互式命令行
- 🖥️ **图形界面**: 基于终端的图形用户界面
- 📊 **结果显示**: 美观的表格形式显示查询结果
- ⚡ **性能统计**: 显示查询执行时间
- 🛠️ **内置命令**: 支持 help、status、quit 等内置命令

## 构建

### Windows
```bash
build.bat
```

### Linux/Mac
```bash
chmod +x build.sh
./build.sh
```

## 使用方法

### 基本用法

```bash
# 使用默认设置连接 (127.0.0.1:3308, root用户, test数据库)
./xmysql-client

# 指定连接参数
./xmysql-client -h localhost -P 3308 -u root -p mypassword -D mydatabase

# 启动图形界面
./xmysql-client --gui
```

### 命令行参数

| 参数 | 长参数 | 说明 | 默认值 |
|------|--------|------|--------|
| `-h` | `--host` | 服务器地址 | 127.0.0.1 |
| `-P` | `--port` | 端口号 | 3308 |
| `-u` | `--user` | 用户名 | root |
| `-p` | `--password` | 密码 | (空) |
| `-D` | `--database` | 数据库名 | test |
| | `--gui` | 启用图形界面 | false |
| | `--help` | 显示帮助 | |

### 内置命令

在命令行模式下，支持以下内置命令：

| 命令 | 别名 | 说明 |
|------|------|------|
| `help` | `\h` | 显示帮助信息 |
| `quit` | `\q`, `exit` | 退出客户端 |
| `status` | `\s` | 显示连接状态 |
| `clear` | `\c` | 清屏 |

### SQL 命令示例

```sql
-- 查看表
SHOW TABLES;

-- 查询数据
SELECT * FROM users LIMIT 10;

-- 插入数据
INSERT INTO users (name, email) VALUES ('张三', 'zhangsan@example.com');

-- 更新数据
UPDATE users SET email = 'newemail@example.com' WHERE id = 1;

-- 删除数据
DELETE FROM users WHERE id = 1;

-- 创建表
CREATE TABLE test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 删除表
DROP TABLE test_table;
```

## 界面模式

### 命令行模式 (默认)

```
欢迎使用 XMySQL 客户端!
连接到: 127.0.0.1:3308
用户: root
数据库: test

输入 'help' 查看帮助，输入 'quit' 或 'exit' 退出。

xmysql> SELECT * FROM users LIMIT 3;
+----+--------+------------------+
| id | name   | email            |
+----+--------+------------------+
| 1  | 张三   | zhang@example.com |
| 2  | 李四   | li@example.com    |
| 3  | 王五   | wang@example.com  |
+----+--------+------------------+
3 rows in set (0.001 sec)

xmysql> 
```

### 图形界面模式

使用 `--gui` 参数启动图形界面，提供：
- SQL 查询输入框
- 结果表格显示
- 状态栏显示连接信息和执行结果

**操作说明:**
- 在输入框中输入 SQL 命令
- 按 `Enter` 执行查询
- 按 `Ctrl+C` 退出程序

## 连接到服务器

确保您的 XMySQL 服务器正在运行：

```bash
# 启动服务器 (在项目根目录)
go run main.go --configPath=conf/my.ini
```

然后使用客户端连接：

```bash
# 连接到本地服务器
./xmysql-client

# 连接到远程服务器
./xmysql-client -h 192.168.1.100 -P 3308 -u myuser -p mypass -D mydb
```

## 故障排除

### 连接失败

1. **检查服务器是否运行**
   ```bash
   netstat -an | grep 3308
   ```

2. **检查防火墙设置**
   确保端口 3308 没有被防火墙阻止

3. **检查连接参数**
   确认主机地址、端口、用户名和密码是否正确

### 编译错误

1. **确保 Go 版本**
   ```bash
   go version  # 需要 Go 1.13 或更高版本
   ```

2. **更新依赖**
   ```bash
   go mod tidy
   ```

## 开发

### 项目结构

```
client/
├── main.go          # 主程序
├── go.mod           # Go 模块文件
├── go.sum           # 依赖校验文件
├── build.bat        # Windows 构建脚本
├── build.sh         # Linux/Mac 构建脚本
└── README.md        # 说明文档
```

### 依赖

- `github.com/go-sql-driver/mysql` - MySQL 驱动
- `github.com/gizak/termui/v3` - 终端 UI 库

## 许可证

本项目遵循与主项目相同的许可证。 