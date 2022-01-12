package basic

type Result struct {
	StatementID int64
	Rows        Rows
	Err         error
	ResultType  string
}

func NewResult() *Result {
	var rows = make([]Row, 0)
	return &Result{
		StatementID: 0,
		Rows:        rows,
		Err:         nil,
	}
}

func (result *Result) AddRows(row Row) {
	result.Rows.AddRow(row)
}
