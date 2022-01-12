package protocol

type ResultSet struct {
	MySQLPacket
	RowNum uint64
	Fields []FieldPacket
	Data   []RowDataPacket
}
