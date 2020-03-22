package sql

import (
	"database/sql"

	"github.com/gopub/log"
)

type TxWrapper struct {
	tx         *sql.Tx
	driverName string
}

func (t *TxWrapper) Commit() error {
	return t.tx.Commit()
}

func (t *TxWrapper) Rollback() error {
	return t.tx.Rollback()
}

func (t *TxWrapper) Table(name string) *Table {
	return &Table{
		exe:        t.tx,
		driverName: t.driverName,
		name:       name,
	}
}

func (t *TxWrapper) Insert(record interface{}) error {
	return t.Table(getTableName(record)).Insert(record)
}

func (t *TxWrapper) Update(record interface{}) error {
	return t.Table(getTableName(record)).Update(record)
}

func (t *TxWrapper) Save(record interface{}) error {
	return t.Table(getTableName(record)).Save(record)
}

func (t *TxWrapper) Select(records interface{}, where string, args ...interface{}) error {
	return t.Table(getTableNameBySlice(records)).Select(records, where, args...)
}

func (t *TxWrapper) SelectOne(record interface{}, where string, args ...interface{}) error {
	return t.Table(getTableName(record)).SelectOne(record, where, args...)
}

func (t *TxWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	log.Debug(query, toReadableArgs(args))
	return t.tx.Exec(query, args...)
}
