package mvcc

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	formatmvcc "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

type RecordVersion = formatmvcc.RecordVersion
type ReadView = formatmvcc.ReadView

func NewRecordVersion(version, txID uint64, key basic.Value, value basic.Row) *RecordVersion {
	return formatmvcc.NewRecordVersion(version, txID, 0, key, value)
}

func NewReadView(txID uint64, activeTxIDs []uint64) *ReadView {
	nextTxID := txID + 1
	for _, activeTxID := range activeTxIDs {
		if activeTxID >= nextTxID {
			nextTxID = activeTxID + 1
		}
	}
	return formatmvcc.NewReadView(activeTxIDs, txID, nextTxID)
}
