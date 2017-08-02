package gosql

import (
	"bytes"
	"database/sql"
	"github.com/natande/gox"
	"sync"
)

type DB struct {
	db *sql.DB
	*executor
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &DB{
		db: db,
		executor: &executor{
			exe:             db,
			typeToFieldInfo: sync.Map{},
			driverName:      driverName,
		},
	}, nil
}

func (d *DB) SQLDB() *sql.DB {
	return d.db
}

func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return d.db.Exec(query, args...)
}

func (d *DB) MustExec(query string, args ...interface{}) {
	_, err := d.db.Exec(query, args...)
	if err != nil {
		panic(err)
	}
}

func (d *DB) Begin() (*Tx, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx: tx,
		executor: &executor{
			exe:             tx,
			typeToFieldInfo: d.executor.typeToFieldInfo,
		},
	}, nil
}

func (d *DB) Count(table string, where string, args ...interface{}) (int, error) {
	var buf bytes.Buffer
	buf.WriteString("SELECT COUNT(*) FROM ")
	buf.WriteString(table)
	if len(where) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
	}
	query := buf.String()
	gox.LogInfo(query, args)

	var count int
	err := d.db.QueryRow(query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}
