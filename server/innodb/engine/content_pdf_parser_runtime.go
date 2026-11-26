package engine

import "github.com/zhukovaskychina/xmysql-server/server/innodb/content"

func (e *XMySQLExecutor) SetContentPDFPageParser(parser content.PDFPageParser) {
	if e == nil {
		return
	}
	e.pdfPageParserMu.Lock()
	e.pdfPageParser = parser
	e.pdfPageParserMu.Unlock()
}

func (e *XMySQLExecutor) contentPDFPageParserSnapshot() content.PDFPageParser {
	if e == nil {
		return content.IsolatedPDFPageParser{}
	}
	e.pdfPageParserMu.RLock()
	parser := e.pdfPageParser
	e.pdfPageParserMu.RUnlock()
	if parser == nil {
		return content.IsolatedPDFPageParser{}
	}
	return parser
}
