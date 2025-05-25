package manager

// TablesGenerator 生成TABLES表数据
type TablesGenerator struct {
	dictManager *DictionaryManager
}

func (g *TablesGenerator) Generate() ([][]interface{}, error) {
	var rows [][]interface{}

	// 遍历所有表定义
	for _, table := range g.dictManager.tables {
		row := make([]interface{}, 21)
		row[0] = "def"                 // TABLE_CATALOG
		row[1] = "test"                // TABLE_SCHEMA
		row[2] = table.Name            // TABLE_NAME
		row[3] = "BASE TABLE"          // TABLE_TYPE
		row[4] = "InnoDB"              // ENGINE
		row[5] = uint64(10)            // VERSION
		row[6] = "Dynamic"             // ROW_FORMAT
		row[7] = uint64(0)             // TABLE_ROWS
		row[8] = uint64(0)             // AVG_ROW_LENGTH
		row[9] = uint64(16384)         // DATA_LENGTH
		row[10] = uint64(0)            // MAX_DATA_LENGTH
		row[11] = uint64(0)            // INDEX_LENGTH
		row[12] = uint64(0)            // DATA_FREE
		row[13] = table.AutoIncr       // AUTO_INCREMENT
		row[14] = table.CreateTime     // CREATE_TIME
		row[15] = table.UpdateTime     // UPDATE_TIME
		row[16] = nil                  // CHECK_TIME
		row[17] = "utf8mb4_general_ci" // TABLE_COLLATION
		row[18] = nil                  // CHECKSUM
		row[19] = ""                   // CREATE_OPTIONS
		row[20] = ""                   // TABLE_COMMENT
		rows = append(rows, row)
	}

	return rows, nil
}

// ColumnsGenerator 生成COLUMNS表数据
type ColumnsGenerator struct {
	dictManager *DictionaryManager
}

func (g *ColumnsGenerator) Generate() ([][]interface{}, error) {
	var rows [][]interface{}

	// 遍历所有表定义
	for _, table := range g.dictManager.tables {
		for i, col := range table.Columns {
			row := make([]interface{}, 20)
			row[0] = "def"                              // TABLE_CATALOG
			row[1] = "test"                             // TABLE_SCHEMA
			row[2] = table.Name                         // TABLE_NAME
			row[3] = col.Name                           // COLUMN_NAME
			row[4] = uint64(i + 1)                      // ORDINAL_POSITION
			row[5] = col.DefaultValue                   // COLUMN_DEFAULT
			row[6] = g.getNullable(col.Nullable)        // IS_NULLABLE
			row[7] = g.getDataType(col.Type)            // DATA_TYPE
			row[8] = uint64(col.Length)                 // CHARACTER_MAXIMUM_LENGTH
			row[9] = uint64(col.Length)                 // CHARACTER_OCTET_LENGTH
			row[10] = nil                               // NUMERIC_PRECISION
			row[11] = nil                               // NUMERIC_SCALE
			row[12] = nil                               // DATETIME_PRECISION
			row[13] = "utf8mb4"                         // CHARACTER_SET_NAME
			row[14] = "utf8mb4_general_ci"              // COLLATION_NAME
			row[15] = g.getColumnType(col)              // COLUMN_TYPE
			row[16] = ""                                // COLUMN_KEY
			row[17] = g.getExtra(col)                   // EXTRA
			row[18] = "select,insert,update,references" // PRIVILEGES
			row[19] = col.Comment                       // COLUMN_COMMENT
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// StatisticsGenerator 生成STATISTICS表数据
type StatisticsGenerator struct {
	indexManager *IndexManager
}

func (g *StatisticsGenerator) Generate() ([][]interface{}, error) {
	var rows [][]interface{}

	// 遍历所有索引
	for _, index := range g.indexManager.indexes {
		for i, col := range index.Columns {
			row := make([]interface{}, 16)
			row[0] = "def"                          // TABLE_CATALOG
			row[1] = "test"                         // TABLE_SCHEMA
			row[2] = g.getTableName(index.TableID)  // TABLE_NAME
			row[3] = g.getNonUnique(index.IsUnique) // NON_UNIQUE
			row[4] = "test"                         // INDEX_SCHEMA
			row[5] = index.Name                     // INDEX_NAME
			row[6] = uint64(i + 1)                  // SEQ_IN_INDEX
			row[7] = col.Name                       // COLUMN_NAME
			row[8] = "A"                            // COLLATION
			row[9] = uint64(0)                      // CARDINALITY
			row[10] = nil                           // SUB_PART
			row[11] = nil                           // PACKED
			row[12] = g.getNullable(col.Nullable)   // NULLABLE
			row[13] = "BTREE"                       // INDEX_TYPE
			row[14] = ""                            // COMMENT
			row[15] = ""                            // INDEX_COMMENT
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// 辅助函数
func (g *ColumnsGenerator) getNullable(nullable bool) string {
	if nullable {
		return "YES"
	}
	return "NO"
}

func (g *ColumnsGenerator) getDataType(typ uint8) string {
	switch typ {
	case MYSQL_TYPE_TINY:
		return "tinyint"
	case MYSQL_TYPE_SHORT:
		return "smallint"
	case MYSQL_TYPE_LONG:
		return "int"
	case MYSQL_TYPE_FLOAT:
		return "float"
	case MYSQL_TYPE_DOUBLE:
		return "double"
	case MYSQL_TYPE_TIMESTAMP:
		return "timestamp"
	case MYSQL_TYPE_LONGLONG:
		return "bigint"
	case MYSQL_TYPE_INT24:
		return "mediumint"
	case MYSQL_TYPE_DATE:
		return "date"
	case MYSQL_TYPE_TIME:
		return "time"
	case MYSQL_TYPE_DATETIME:
		return "datetime"
	case MYSQL_TYPE_YEAR:
		return "year"
	case MYSQL_TYPE_VARCHAR:
		return "varchar"
	case MYSQL_TYPE_BIT:
		return "bit"
	case MYSQL_TYPE_JSON:
		return "json"
	case MYSQL_TYPE_NEWDECIMAL:
		return "decimal"
	case MYSQL_TYPE_ENUM:
		return "enum"
	case MYSQL_TYPE_SET:
		return "set"
	case MYSQL_TYPE_TINY_BLOB:
		return "tinyblob"
	default:
		return "unknown"
	}
}

func (g *ColumnsGenerator) getColumnType(col ColumnDef) string {
	typ := g.getDataType(col.Type)
	if col.Length > 0 {
		return typ + "(" + string(col.Length) + ")"
	}
	return typ
}

func (g *ColumnsGenerator) getExtra(col ColumnDef) string {
	if col.OnUpdate != "" {
		return "on update " + col.OnUpdate
	}
	return ""
}

func (g *StatisticsGenerator) getNonUnique(unique bool) uint8 {
	if unique {
		return 0
	}
	return 1
}

func (g *StatisticsGenerator) getTableName(tableID uint64) string {
	// 这里需要通过tableID查找表名
	// 实际实现中应该通过DictionaryManager查找
	return "unknown"
}

func (g *StatisticsGenerator) getNullable(nullable bool) interface{} {
	return nil
}
