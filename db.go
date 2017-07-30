package gosql

import (
	"database/sql"
	"sync"
)

type DB struct {
	db *sql.DB
	*executor
}

func NewDB(db *sql.DB) *DB {
	return &DB{
		db: db,
		executor: &executor{
			exe:             db,
			typeToFieldInfo: sync.Map{},
		},
	}
}

func (d *DB) RawDB() *sql.DB {
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
