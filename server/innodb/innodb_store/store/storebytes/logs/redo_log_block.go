package logs

import "github.com/zhukovaskychina/xmysql-server/util"

type RedoLogBlock struct {
	LogBlockHeader  LogBlockHeader  //12byte
	LogBlockBody    LogBlockBody    //508-12
	LogBlockTrailer LogBlockTrailer //4byte
}

type LogBlockHeader struct {
	LogBlockHdrNo         []byte //4 byte  每一个Block都有一个大雨
	LogBlockHdrDataLen    []byte // 2byte
	LogBlockFirstRecGroup []byte //2byte
	LogBlockCheckPointNo  []byte //4 byte
}

type LogBlockTrailer struct {
	LogBlockCheckSum []byte //4 byte
}

//508-12
type LogBlockBody struct {
}

//前四个block存储一些管理信息
//
//
//
//
//
type LogBuffer struct {
	RedoLogBlockBuffers []RedoLogBlock
}

//前四个logfilede 结构

type RedoLogBlockPreFour struct {
	LogBlockHeader    LogBlockHeader  //12byte
	LogHeaderPad1     []byte          //4 byte 字节填充，没有意义
	LogHeaderStartLsn []byte          //8 byte   标记redo日志文件偏移量2048字节处对应的lsn值
	LogHeaderCreator  []byte          //32 byte 一个字符串，标记本redo日志文件的创建者
	LogBlockTrailer   LogBlockTrailer //4byte
}

type CheckPoint1 struct {
	LogCheckPointNo          []byte //8 byte  服务器执行checkpoint 的编号，每执行一次checkpoint，该值就+1
	LogCheckPointLsn         []byte //8byte  服务器在结束checkpoint对应的lsn值，系统在崩溃后会从该值开始
	LogCheckPointOffset      []byte //8 byte	上个属性中的lsn值在redo日志文件组中的偏移量
	LogCheckPointLogBuffSize []byte //8byte	服务器在执行checkpoint操作时对应的logbuffer大小
	LogBlockCheckSum         []byte //4 byte	//本block的校验值
	NotUsedBytes             []byte //508-32byte
	LogBlockTrailer
}

type CheckPoint2 struct {
	LogCheckPointNo          []byte //8 byte  服务器执行checkpoint 的编号，每执行一次checkpoint，该值就+1
	LogCheckPointLsn         []byte //8byte  服务器在结束checkpoint对应的lsn值，系统在崩溃后会从该值开始
	LogCheckPointOffset      []byte //8 byte	上个属性中的lsn值在redo日志文件组中的偏移量
	LogCheckPointLogBuffSize []byte //8byte	服务器在执行checkpoint操作时对应的logbuffer大小
	LogBlockCheckSum         []byte //4 byte	//本block的校验值
	NotUsedBytes             []byte //508-32byte
	LogBlockTrailer
}

func CalcLogBlockHrdNo(lsn uint64) []byte {
	result := ((lsn / 512) & 0x3FFFFFFF) + 1
	return util.ConvertInt4Bytes(int32(result))
}
