package engine

import "github.com/zhukovaskychina/xmysql-server/server/innodb/content"

func (e *XMySQLExecutor) SetContentXLSXRowParser(parser content.XLSXRowParser) {
	if e == nil {
		return
	}
	e.xlsxRowParserMu.Lock()
	e.xlsxRowParser = parser
	e.xlsxRowParserMu.Unlock()
}

func (e *XMySQLExecutor) contentXLSXRowParserSnapshot() content.XLSXRowParser {
	if e == nil {
		return content.IsolatedXLSXRowParser{}
	}
	e.xlsxRowParserMu.RLock()
	parser := e.xlsxRowParser
	e.xlsxRowParserMu.RUnlock()
	if parser == nil {
		return content.IsolatedXLSXRowParser{}
	}
	return parser
}
