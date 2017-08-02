package gosql

import "database/sql"

type Rows sql.Rows

func (r *Rows) Get(records interface{}) error {
	return nil
}

type Row sql.Row

func (r *Row) Get(record interface{}) error {
	return nil
}
