package store

import (
	"errors"
	"io"
	"xmysql-server/server/innodb/basic"
)

type MemoryIterator struct {
	basic.RowIterator
	index        int
	currentRow   basic.Row
	iterator     basic.Iterator
	tmpIteraotor basic.Iterator
}

func NewMemoryIterator(iterator basic.Iterator) basic.RowIterator {
	var memoryIterator = new(MemoryIterator)
	memoryIterator.iterator = iterator
	memoryIterator.index = 0
	return memoryIterator
}

func (m *MemoryIterator) Open() error {

	_, _, currentRow, err, tbIterator := m.iterator()
	if err != nil {
		return err
	}
	if currentRow == nil {
		return errors.New("查无实据")
	}
	m.tmpIteraotor = tbIterator
	m.currentRow = currentRow
	return nil
}

func (m *MemoryIterator) GetCurrentRow() (basic.Row, error) {

	return m.currentRow, nil
}

func (m *MemoryIterator) Next() (basic.Row, error) {
	if m.tmpIteraotor == nil {
		return nil, io.EOF
	}
	var resultError error
	_, _, m.currentRow, resultError, m.tmpIteraotor = m.tmpIteraotor()

	return m.currentRow, resultError
}

func (m *MemoryIterator) Close() error {
	m.tmpIteraotor = nil
	m.index = 0
	return nil
}
