package gosql

import "database/sql"

type Tx struct {
	tx *sql.Tx
	*executor
}

func (t *Tx) Commit() error {
	return t.tx.Commit()
}

func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}
