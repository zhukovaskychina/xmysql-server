package space

import "sync"

// spaceTx implements the Tx interface for space transactions
type spaceTx struct {
	sync.Mutex
	id        uint64
	committed bool
	writes    []func()
}

// newSpaceTx creates a new space transaction
func newSpaceTx(id uint64) *spaceTx {
	return &spaceTx{
		id:        id,
		committed: false,
		writes:    make([]func(), 0),
	}
}

// AddWrite adds a write operation to the transaction
func (tx *spaceTx) AddWrite(writeFn func()) {
	tx.Lock()
	defer tx.Unlock()

	tx.writes = append(tx.writes, writeFn)
}

// Commit commits all write operations in the transaction
func (tx *spaceTx) Commit() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.committed {
		return nil
	}

	// Execute all write operations
	for _, write := range tx.writes {
		write()
	}

	tx.committed = true
	return nil
}

// Rollback discards all write operations
func (tx *spaceTx) Rollback() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.committed {
		return nil
	}

	// Clear write operations
	tx.writes = tx.writes[:0]
	return nil
}
