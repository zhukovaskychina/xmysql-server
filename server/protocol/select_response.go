package protocol

type Field struct {
	Name  string
	Types int
}

type SelectResponse struct {
	MySQLPacket
	FieldCount int
	Header     *ResultSetHeaderPacket
	Fields     []Field
	EOFPacket  EOFPacket
	PackId     byte
}

func NewSelectResponse(fieldCount int) *SelectResponse {
	selectResponse := new(SelectResponse)
	selectResponse.FieldCount = fieldCount
	selectResponse.Header = GetResultSetHeaderPacket(fieldCount)
	selectResponse.Header.PacketId = 1
	selectResponse.Fields = make([]Field, 0)
	selectResponse.EOFPacket = NewEOFPacket()
	//	selectResponse.Rows=make()

	return selectResponse
}

func (sp *SelectResponse) AddField(fieldName string, fieldType int) {
	sp.Fields = append(sp.Fields, Field{
		Name:  fieldName,
		Types: fieldType,
	})

}

func (sp *SelectResponse) EncodeEof() []byte {
	sp.PackId++
	sp.EOFPacket.PacketId = sp.PackId
	return sp.EOFPacket.WriteEOF()
}

func (sp *SelectResponse) EncodeLastEof() []byte {
	eof := NewEOFPacket()
	sp.PackId++
	eof.PacketId = sp.PackId
	return eof.WriteEOF()
}

func (sp *SelectResponse) WriteStringRows(data []string) []byte {
	row := NewRowDataPacket(sp.FieldCount)
	i := 0
	for i = 0; i < len(data); i++ {
		row.Add([]byte(data[i]))
	}
	sp.PackId++
	row.PacketId = sp.PackId
	return row.EncodeRowPacket()
}

func (sp *SelectResponse) EncodeFields() []byte {
	buff := make([]byte, 0)
	i := 0
	for i = 0; i < len(sp.Fields); i++ {
		packet := GetField(sp.Fields[i].Name, sp.Fields[i].Types)
		sp.PackId++
		packet.PacketId = sp.PackId
		buff = append(buff, packet.EncodeFieldPacket()...)
	}

	return buff
}
