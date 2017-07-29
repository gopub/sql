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
			se:              db,
			typeToFieldInfo: sync.Map{},
		},
	}
}

func (d *DB) CreateTable(table string, schema interface{}) error {
	return nil
}

func (d *DB) MustCreateTable(table string, schema interface{}) {

}
