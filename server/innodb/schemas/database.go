package schemas

type Database interface {
	Name() string

	GetTable(name string) (Table, error)

	ListTables() []Table

	//CreateTable(conf *conf.Cfg, stmt *sqlparser.DDL) (Table, error)

	DropTable(name string) error

	ListTableName() []string
}
