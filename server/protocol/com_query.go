package protocol

func EncodeQuery(sql string) []byte {
	buff := []byte{}
	buff = append(buff, 0x03)
	for _, v := range []byte(sql) {
		buff = append(buff, v)
	}
	buff = append(buff, 0)
	return buff
}
