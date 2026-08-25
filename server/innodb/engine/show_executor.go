package engine

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ShowExecutor SHOW语句执行器
// 注意：此执行器用于处理特殊的SHOW语句，不是火山模型的Operator
type ShowExecutor struct {
	schema            *metadata.Table
	children          []interface{} // 改为interface{}以避免循环依赖
	showType          string
	schemaName        string
	tableName         string
	infoSchemaManager metadata.InfoSchemaManager
	rows              [][]interface{}
	current           int
	closed            bool
}

// buildShowExecutor 构建SHOW语句执行器
func (e *XMySQLExecutor) buildShowExecutor(showType string) *ShowExecutor {
	executor := &ShowExecutor{
		schema:            nil,
		children:          nil,
		showType:          showType,
		infoSchemaManager: e.infosSchemaManager,
		rows:              make([][]interface{}, 0),
		current:           -1,
	}
	return executor
}

// Schema 返回算子的输出模式
func (e *ShowExecutor) Schema() *metadata.Table {
	if e.schema != nil {
		return e.schema
	}
	e.schema = e.buildSchema()
	return e.schema
}

// Children 返回子算子（用于兼容性）
func (e *ShowExecutor) Children() []interface{} {
	return e.children
}

// SetChildren 设置子算子（用于兼容性）
func (e *ShowExecutor) SetChildren(children []interface{}) {
	e.children = children
}

// Init 初始化SHOW执行器
func (e *ShowExecutor) Init() error {
	if e.closed {
		return nil
	}

	// 根据SHOW类型获取数据
	var err error
	switch strings.ToUpper(strings.TrimSpace(e.showType)) {
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
	if e.infoSchemaManager == nil {
		return nil, fmt.Errorf("SHOW DATABASES requires information schema manager")
	}

	schemas, err := e.infoSchemaManager.GetAllSchemaNames(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load database metadata: %w", err)
	}
	sort.Strings(schemas)

	rows := make([][]interface{}, 0, len(schemas))
	for _, schemaName := range schemas {
		rows = append(rows, []interface{}{schemaName})
	}
	return rows, nil
}

// showTables 获取当前数据库的表列表
func (e *ShowExecutor) showTables() ([][]interface{}, error) {
	if e.infoSchemaManager == nil {
		return nil, fmt.Errorf("SHOW TABLES requires information schema manager")
	}
	if strings.TrimSpace(e.schemaName) == "" {
		return nil, fmt.Errorf("database name is required for SHOW TABLES")
	}

	tables, err := e.infoSchemaManager.GetAllTables(context.Background(), e.schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to load table metadata for database %s: %w", e.schemaName, err)
	}
	sort.Slice(tables, func(i, j int) bool {
		return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name)
	})

	rows := make([][]interface{}, 0, len(tables))
	for _, table := range tables {
		if table == nil {
			continue
		}
		rows = append(rows, []interface{}{table.Name})
	}
	return rows, nil
}

// showColumns 获取指定表的列信息
func (e *ShowExecutor) showColumns() ([][]interface{}, error) {
	if e.infoSchemaManager == nil {
		return nil, fmt.Errorf("SHOW COLUMNS requires information schema manager")
	}
	if strings.TrimSpace(e.schemaName) == "" {
		return nil, fmt.Errorf("database name is required for SHOW COLUMNS")
	}
	if strings.TrimSpace(e.tableName) == "" {
		return nil, fmt.Errorf("table name is required for SHOW COLUMNS")
	}

	tableMeta, err := e.infoSchemaManager.GetTableMetadata(context.Background(), e.schemaName, e.tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to load column metadata for %s.%s: %w", e.schemaName, e.tableName, err)
	}
	if tableMeta == nil {
		return nil, fmt.Errorf("table metadata is nil for %s.%s", e.schemaName, e.tableName)
	}

	rows := make([][]interface{}, 0, len(tableMeta.Columns))
	for _, col := range tableMeta.Columns {
		if col == nil {
			continue
		}

		nullText := "YES"
		if !col.IsNullable {
			nullText = "NO"
		}

		keyText := ""
		if col.IsPrimary {
			keyText = "PRI"
		} else if col.IsUnique {
			keyText = "UNI"
		}

		extraText := ""
		if col.IsAutoIncrement {
			extraText = "auto_increment"
		}

		rows = append(rows, []interface{}{
			col.Name,
			formatShowExecutorColumnType(col.Type, col.Length),
			nullText,
			keyText,
			col.DefaultValue,
			extraText,
		})
	}
	return rows, nil
}

func (e *ShowExecutor) buildSchema() *metadata.Table {
	switch strings.ToUpper(strings.TrimSpace(e.showType)) {
	case "DATABASES":
		return newShowExecutorSchema("show_databases", []string{"Database"})
	case "TABLES":
		columnName := "Tables_in_" + strings.TrimSpace(e.schemaName)
		if strings.TrimSpace(e.schemaName) == "" {
			columnName = "Tables"
		}
		return newShowExecutorSchema("show_tables", []string{columnName})
	case "COLUMNS":
		return newShowExecutorSchema("show_columns", []string{"Field", "Type", "Null", "Key", "Default", "Extra"})
	default:
		return newShowExecutorSchema("show", []string{"Value"})
	}
}

func newShowExecutorSchema(name string, columnNames []string) *metadata.Table {
	table := metadata.NewTable(name)
	for _, columnName := range columnNames {
		table.AddColumn(&metadata.Column{
			Name:          columnName,
			DataType:      metadata.TypeVarchar,
			CharMaxLength: 255,
			IsNullable:    true,
		})
	}
	return table
}

func formatShowExecutorColumnType(dataType metadata.DataType, length int) string {
	if length > 0 {
		switch dataType {
		case metadata.TypeChar, metadata.TypeVarchar, metadata.TypeBinary, metadata.TypeVarBinary:
			return fmt.Sprintf("%s(%d)", strings.ToLower(string(dataType)), length)
		}
	}
	return strings.ToLower(string(dataType))
}
