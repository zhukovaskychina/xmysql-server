package protocol

import "xmysql-server/util"

type EventRotateBody struct {
	Offset         uint64
	BinlogFileName string
}

type EventRotate struct {
	Header EventHeader
	Body   EventRotateBody
}

func DecodeRotate(buf []byte) EventRotate {
	r := new(EventRotate)
	r.Header = DecodeEventHeader(buf)

	var c int = 20
	c, r.Body.Offset = util.ReadUB8(buf, c)
	c, r.Body.BinlogFileName = util.ReadString(buf, c)
	return *r
}
