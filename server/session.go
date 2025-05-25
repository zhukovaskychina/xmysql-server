package server

import (
	"time"
)

//
type MySQLServerSession interface {

	//获得当前链接最后一次活跃的时间

	GetLastActiveTime() time.Time

	SendOK()

	SendHandleOk()

	GetParamByName(name string) interface{}

	SetParamByName(name string, value interface{})

	SendSelectFields()
}
