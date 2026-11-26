package engine

import "github.com/zhukovaskychina/xmysql-server/server/innodb/content"

func (e *XMySQLExecutor) SetContentDOCXBlockParser(parser content.DOCXBlockParser) {
	if e == nil {
		return
	}
	e.docxBlockParserMu.Lock()
	e.docxBlockParser = parser
	e.docxBlockParserMu.Unlock()
}

func (e *XMySQLExecutor) contentDOCXBlockParserSnapshot() content.DOCXBlockParser {
	if e == nil {
		return content.IsolatedDOCXBlockParser{}
	}
	e.docxBlockParserMu.RLock()
	parser := e.docxBlockParser
	e.docxBlockParserMu.RUnlock()
	if parser == nil {
		return content.IsolatedDOCXBlockParser{}
	}
	return parser
}
