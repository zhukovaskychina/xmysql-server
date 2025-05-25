package engine

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"io"
)

// ShowExecutor SHOW语句执行器
type ShowExecutor struct {
	BaseExecutor
	showType string
	rows     [][]interface{}
	current  int
}

// buildShowExecutor 构建SHOW语句执行器
func (e *XMySQLExecutor) buildShowExecutor(showType string) Executor {
	executor := &ShowExecutor{
		BaseExecutor: BaseExecutor{
			ctx:    e.ctx,
			schema: nil, // Schema是接口，设为nil
		},
		showType: showType,
		rows:     make([][]interface{}, 0),
		current:  -1,
	}
	return executor
}

// Schema 返回算子的输出模式
func (e *ShowExecutor) Schema() *metadata.Schema {
	return nil // 简化实现，返回nil
}

// Children 返回子算子
func (e *ShowExecutor) Children() []Executor {
	return e.children
}

// SetChildren 设置子算子
func (e *ShowExecutor) SetChildren(children []Executor) {
	e.children = children
}

// Init 初始化SHOW执行器
func (e *ShowExecutor) Init() error {
	if e.closed {
		return nil
	}

	// 根据SHOW类型获取数据
	var err error
	switch e.showType {
	case "DATABASES":
		e.rows, err = e.showDatabases()
	case "TABLES":
		e.rows, err = e.showTables()
	case "COLUMNS":
		e.rows, err = e.showColumns()
	default:
		return fmt.Errorf("unsupported SHOW type: %s", e.showType)
	}

	return err
}

// Next 获取下一行数据
func (e *ShowExecutor) Next() error {
	if e.closed {
		return io.EOF
	}

	e.current++
	if e.current >= len(e.rows) {
		return io.EOF
	}

	return nil
}

// GetRow 获取当前行数据
func (e *ShowExecutor) GetRow() []interface{} {
	if e.current < 0 || e.current >= len(e.rows) {
		return nil
	}
	return e.rows[e.current]
}

// Close 关闭执行器
func (e *ShowExecutor) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.rows = nil
	return nil
}

// showDatabases 获取所有数据库列表
func (e *ShowExecutor) showDatabases() ([][]interface{}, error) {
	// TODO: 从InfoSchemaManager获取数据库列表
	return [][]interface{}{
		{"information_schema"},
		{"mysql"},
		{"test"},
	}, nil
}

// showTables 获取当前数据库的表列表
func (e *ShowExecutor) showTables() ([][]interface{}, error) {
	// TODO: 从InfoSchemaManager获取表列表
	return [][]interface{}{
		{"users"},
		{"orders"},
	}, nil
}

// showColumns 获取指定表的列信息
func (e *ShowExecutor) showColumns() ([][]interface{}, error) {
	// TODO: 从InfoSchemaManager获取列信息
	return [][]interface{}{
		{"id", "int", "NO", "PRI", nil, "auto_increment"},
		{"name", "varchar(100)", "YES", "", nil, ""},
	}, nil
}
