package engine

import "github.com/zhukovaskychina/xmysql-server/server/innodb/content"

func (e *XMySQLExecutor) SetContentHTMLBlockParser(parser content.HTMLBlockParser) {
	if e == nil {
		return
	}
	e.htmlBlockParserMu.Lock()
	e.htmlBlockParser = parser
	e.htmlBlockParserMu.Unlock()
}

func (e *XMySQLExecutor) contentHTMLBlockParserSnapshot() content.HTMLBlockParser {
	if e == nil {
		return content.InProcessHTMLBlockParser{}
	}
	e.htmlBlockParserMu.RLock()
	parser := e.htmlBlockParser
	e.htmlBlockParserMu.RUnlock()
	if parser == nil {
		return content.InProcessHTMLBlockParser{}
	}
	return parser
}
