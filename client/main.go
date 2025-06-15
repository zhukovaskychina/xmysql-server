package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	ui "1/gizak/termui/v3"
	_ "1/go-sql-driver/mysql"
	"github.com/gizak/termui/v3/widgets"
)

// ClientConfig 客户端配置
type ClientConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// MySQLClient MySQL客户端
type MySQLClient struct {
	config *ClientConfig
	db     *sql.DB
	isGUI  bool
}

// NewMySQLClient 创建新的MySQL客户端
func NewMySQLClient(config *ClientConfig, isGUI bool) *MySQLClient {
	return &MySQLClient{
		config: config,
		isGUI:  isGUI,
	}
}

// Connect 连接到MySQL服务器
func (c *MySQLClient) Connect() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		c.config.User,
		c.config.Password,
		c.config.Host,
		c.config.Port,
		c.config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return fmt.Errorf("无法连接到服务器: %v", err)
	}

	c.db = db
	return nil
}

// Close 关闭连接
func (c *MySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// ExecuteQuery 执行查询
func (c *MySQLClient) ExecuteQuery(query string) (*QueryResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("查询不能为空")
	}

	// 判断是否为SELECT查询
	if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		return c.executeSelect(query)
	} else {
		return c.executeNonSelect(query)
	}
}

// QueryResult 查询结果
type QueryResult struct {
	Columns     []string
	Rows        [][]string
	RowsCount   int64
	Message     string
	IsSelect    bool
	ExecuteTime time.Duration
}

// executeSelect 执行SELECT查询
func (c *MySQLClient) executeSelect(query string) (*QueryResult, error) {
	start := time.Now()

	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询执行失败: %v", err)
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}

	// 读取数据
	var data [][]string
	for rows.Next() {
		// 创建接收数据的切片
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("扫描行数据失败: %v", err)
		}

		// 转换为字符串
		row := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取数据时出错: %v", err)
	}

	executeTime := time.Since(start)

	return &QueryResult{
		Columns:     columns,
		Rows:        data,
		RowsCount:   int64(len(data)),
		IsSelect:    true,
		ExecuteTime: executeTime,
		Message:     fmt.Sprintf("%d rows in set (%.3f sec)", len(data), executeTime.Seconds()),
	}, nil
}

// executeNonSelect 执行非SELECT查询
func (c *MySQLClient) executeNonSelect(query string) (*QueryResult, error) {
	start := time.Now()

	result, err := c.db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("执行失败: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	executeTime := time.Since(start)

	var message string
	if strings.HasPrefix(strings.ToUpper(query), "INSERT") {
		message = fmt.Sprintf("Query OK, %d row(s) affected (%.3f sec)", rowsAffected, executeTime.Seconds())
	} else if strings.HasPrefix(strings.ToUpper(query), "UPDATE") {
		message = fmt.Sprintf("Query OK, %d row(s) affected (%.3f sec)", rowsAffected, executeTime.Seconds())
	} else if strings.HasPrefix(strings.ToUpper(query), "DELETE") {
		message = fmt.Sprintf("Query OK, %d row(s) affected (%.3f sec)", rowsAffected, executeTime.Seconds())
	} else {
		message = fmt.Sprintf("Query OK (%.3f sec)", executeTime.Seconds())
	}

	return &QueryResult{
		RowsCount:   rowsAffected,
		IsSelect:    false,
		ExecuteTime: executeTime,
		Message:     message,
	}, nil
}

// StartCLI 启动命令行界面
func (c *MySQLClient) StartCLI() {
	util.Debugf("欢迎使用 XMySQL 客户端!\n")
	util.Debugf("连接到: %s:%d\n", c.config.Host, c.config.Port)
	util.Debugf("用户: %s\n", c.config.User)
	util.Debugf("数据库: %s\n\n", c.config.Database)
	util.Debugf("输入 'help' 查看帮助，输入 'quit' 或 'exit' 退出。\n\n")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("xmysql> ")

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			continue
		}

		// 处理特殊命令
		switch strings.ToLower(input) {
		case "quit", "exit", "\\q":
			fmt.Println("再见!")
			return
		case "help", "\\h":
			c.showHelp()
			continue
		case "status", "\\s":
			c.showStatus()
			continue
		case "clear", "\\c":
			fmt.Print("\033[2J\033[H") // 清屏
			continue
		}

		// 执行SQL查询
		result, err := c.ExecuteQuery(input)
		if err != nil {
			util.Debugf("错误: %v\n\n", err)
			continue
		}

		c.displayResult(result)
	}
}

// StartGUI 启动图形界面
func (c *MySQLClient) StartGUI() {
	if err := ui.Init(); err != nil {
		log.Fatalf("初始化UI失败: %v", err)
	}
	defer ui.Close()

	// 创建输入框
	inputBox := widgets.NewParagraph()
	inputBox.Title = "SQL 查询 (按 Enter 执行, Ctrl+C 退出)"
	inputBox.Text = "SELECT * FROM information_schema.tables LIMIT 10;"
	inputBox.SetRect(0, 0, 80, 5)
	inputBox.BorderStyle = ui.NewStyle(ui.ColorCyan)

	// 创建结果表格
	resultTable := widgets.NewTable()
	resultTable.Title = "查询结果"
	resultTable.SetRect(0, 5, 120, 25)
	resultTable.TextStyle = ui.NewStyle(ui.ColorWhite)
	resultTable.BorderStyle = ui.NewStyle(ui.ColorGreen)
	resultTable.RowSeparator = true
	resultTable.FillRow = true

	// 创建状态栏
	statusBar := widgets.NewParagraph()
	statusBar.Title = "状态"
	statusBar.Text = fmt.Sprintf("连接到: %s:%d | 用户: %s | 数据库: %s",
		c.config.Host, c.config.Port, c.config.User, c.config.Database)
	statusBar.SetRect(0, 25, 120, 30)
	statusBar.BorderStyle = ui.NewStyle(ui.ColorYellow)

	// 初始渲染
	ui.Render(inputBox, resultTable, statusBar)

	// 事件循环
	uiEvents := ui.PollEvents()
	currentQuery := inputBox.Text

	for {
		e := <-uiEvents
		switch e.ID {
		case "q", "<C-c>":
			return
		case "<Enter>":
			// 执行查询
			result, err := c.ExecuteQuery(currentQuery)
			if err != nil {
				statusBar.Text = fmt.Sprintf("错误: %v", err)
				statusBar.BorderStyle = ui.NewStyle(ui.ColorRed)
			} else {
				c.displayGUIResult(result, resultTable)
				statusBar.Text = result.Message
				statusBar.BorderStyle = ui.NewStyle(ui.ColorGreen)
			}
			ui.Render(inputBox, resultTable, statusBar)
		case "<Backspace>":
			if len(currentQuery) > 0 {
				currentQuery = currentQuery[:len(currentQuery)-1]
				inputBox.Text = currentQuery
				ui.Render(inputBox)
			}
		default:
			if len(e.ID) == 1 && e.ID[0] >= 32 && e.ID[0] <= 126 {
				currentQuery += e.ID
				inputBox.Text = currentQuery
				ui.Render(inputBox)
			}
		}
	}
}

// displayResult 显示查询结果（CLI模式）
func (c *MySQLClient) displayResult(result *QueryResult) {
	if result.IsSelect && len(result.Rows) > 0 {
		// 显示表格
		c.printTable(result.Columns, result.Rows)
	}

	util.Debugf("%s\n\n", result.Message)
}

// displayGUIResult 显示查询结果（GUI模式）
func (c *MySQLClient) displayGUIResult(result *QueryResult, table *widgets.Table) {
	if result.IsSelect && len(result.Rows) > 0 {
		// 准备表格数据
		rows := [][]string{result.Columns}
		rows = append(rows, result.Rows...)
		table.Rows = rows

		// 设置表头样式
		table.RowStyles = make(map[int]ui.Style)
		table.RowStyles[0] = ui.NewStyle(ui.ColorWhite, ui.ColorBlue, ui.ModifierBold)
	} else {
		table.Rows = [][]string{{"执行完成"}}
	}
}

// printTable 打印表格（CLI模式）
func (c *MySQLClient) printTable(columns []string, rows [][]string) {
	if len(rows) == 0 {
		return
	}

	// 计算每列的最大宽度
	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	for _, row := range rows {
		for i, cell := range row {
			if i < len(colWidths) && len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// 打印分隔线
	c.printSeparator(colWidths)

	// 打印表头
	fmt.Print("|")
	for i, col := range columns {
		util.Debugf(" %-*s |", colWidths[i], col)
	}
	fmt.Println()

	c.printSeparator(colWidths)

	// 打印数据行
	for _, row := range rows {
		fmt.Print("|")
		for i, cell := range row {
			if i < len(colWidths) {
				util.Debugf(" %-*s |", colWidths[i], cell)
			}
		}
		fmt.Println()
	}

	c.printSeparator(colWidths)
}

// printSeparator 打印分隔线
func (c *MySQLClient) printSeparator(colWidths []int) {
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2) + "+")
	}
	fmt.Println()
}

// showHelp 显示帮助信息
func (c *MySQLClient) showHelp() {
	fmt.Println("XMySQL 客户端帮助:")
	fmt.Println("  help, \\h     显示此帮助")
	fmt.Println("  quit, \\q     退出客户端")
	fmt.Println("  exit         退出客户端")
	fmt.Println("  status, \\s   显示连接状态")
	fmt.Println("  clear, \\c    清屏")
	fmt.Println()
	fmt.Println("SQL 命令:")
	fmt.Println("  SELECT * FROM table_name;")
	fmt.Println("  INSERT INTO table_name VALUES (...);")
	fmt.Println("  UPDATE table_name SET column=value WHERE condition;")
	fmt.Println("  DELETE FROM table_name WHERE condition;")
	fmt.Println("  CREATE TABLE table_name (...);")
	fmt.Println("  DROP TABLE table_name;")
	fmt.Println()
}

// showStatus 显示连接状态
func (c *MySQLClient) showStatus() {
	util.Debugf("连接状态:\n")
	util.Debugf("  服务器: %s:%d\n", c.config.Host, c.config.Port)
	util.Debugf("  用户: %s\n", c.config.User)
	util.Debugf("  数据库: %s\n", c.config.Database)

	if c.db != nil {
		if err := c.db.Ping(); err == nil {
			util.Debugf("  状态: 已连接\n")
		} else {
			util.Debugf("  状态: 连接断开 (%v)\n", err)
		}
	} else {
		util.Debugf("  状态: 未连接\n")
	}
	fmt.Println()
}

// parseArgs 解析命令行参数
func parseArgs() (*ClientConfig, bool, bool) {
	config := &ClientConfig{
		Host:     "127.0.0.1",
		Port:     3307, // 默认使用您服务器的端口
		User:     "root",
		Password: "",
		Database: "test_simple_protocol",
	}

	isGUI := false
	showHelp := false

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--host":
			if i+1 < len(args) {
				config.Host = args[i+1]
				i++
			}
		case "-P", "--port":
			if i+1 < len(args) {
				if port, err := strconv.Atoi(args[i+1]); err == nil {
					config.Port = port
				}
				i++
			}
		case "-u", "--user":
			if i+1 < len(args) {
				config.User = args[i+1]
				i++
			}
		case "-p", "--password":
			if i+1 < len(args) {
				config.Password = args[i+1]
				i++
			}
		case "-D", "--database":
			if i+1 < len(args) {
				config.Database = args[i+1]
				i++
			}
		case "--gui":
			isGUI = true
		case "--help":
			showHelp = true
		}
	}

	return config, isGUI, showHelp
}

// printUsage 打印使用说明
func printUsage() {
	fmt.Println("XMySQL 客户端 - 连接到 XMySQL 服务器")
	fmt.Println()
	fmt.Println("用法:")
	fmt.Println("  xmysql-client [选项]")
	fmt.Println()
	fmt.Println("选项:")
	fmt.Println("  -h, --host HOST      服务器地址 (默认: 127.0.0.1)")
	fmt.Println("  -P, --port PORT      端口号 (默认: 3308)")
	fmt.Println("  -u, --user USER      用户名 (默认: root)")
	fmt.Println("  -p, --password PASS  密码")
	fmt.Println("  -D, --database DB    数据库名 (默认: test_simple_protocol)")
	fmt.Println("  --gui                启用图形界面")
	fmt.Println("  --help               显示此帮助")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  xmysql-client -h localhost -P 3308 -u root -p mypass -D mydb")
	fmt.Println("  xmysql-client --gui")
	fmt.Println()
}

func main() {
	config, isGUI, showHelpFlag := parseArgs()

	if showHelpFlag {
		printUsage()
		return
	}

	// 创建客户端
	client := NewMySQLClient(config, isGUI)

	// 连接到服务器
	util.Debugf("正在连接到 %s:%d...\n", config.Host, config.Port)
	if err := client.Connect(); err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	fmt.Println("连接成功!")

	// 启动相应的界面
	if isGUI {
		client.StartGUI()
	} else {
		client.StartCLI()
	}
}
