package store

//FormMagicHeader    byte   //0xFE           1
//MySQLVersionId     []byte //50732			 4
//RowType            byte   //row_type_compact 1
//TableNameOffSet    []byte //				 4
//TableName          []byte //		不定长
//DataBaseNameOffSet []byte //		4
//DataBaseName       []byte //     不定长
//
//ColumnsLength          []byte //列长度		2byte
//FieldColumnsOffset     []byte //列存储偏移量
//FieldColumns           []byte //实质内容	//FormColumns 数组
//ClusterIndexOffSet     []byte //4
//ClusterIndex           []byte //不定长
//SecondaryIndexesCount  byte
//SecondaryIndexesOffset []byte //4
//SecondaryIndexes       []byte

const (
	FormMagicHeaderOffset = 1
	MySQLVersionIdOffset  = 4
	RowTypeOffset         = 1
	TableNameOffset       = 4
	DatabaseNameOffset    = 4
	ColumnNameOffset      = 2
)

///* States of a descriptor */
//#define	XDES_FREE		1	/* extent is in free list of space */
//#define	XDES_FREE_FRAG		2	/* extent is in free fragment list of
//space */
//#define	XDES_FULL_FRAG		3	/* extent is in full fragment list of
//space */
//#define	XDES_FSEG		4	/* extent belongs to a segment */
