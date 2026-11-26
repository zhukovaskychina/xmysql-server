package engine

import "github.com/zhukovaskychina/xmysql-server/server/innodb/content"

func (e *XMySQLExecutor) SetContentPPTXBlockParser(parser content.PPTXBlockParser) {
	if e == nil {
		return
	}
	e.pptxBlockParserMu.Lock()
	e.pptxBlockParser = parser
	e.pptxBlockParserMu.Unlock()
}

func (e *XMySQLExecutor) contentPPTXBlockParserSnapshot() content.PPTXBlockParser {
	if e == nil {
		return content.IsolatedPPTXBlockParser{}
	}
	e.pptxBlockParserMu.RLock()
	parser := e.pptxBlockParser
	e.pptxBlockParserMu.RUnlock()
	if parser == nil {
		return content.IsolatedPPTXBlockParser{}
	}
	return parser
}
