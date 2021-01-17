package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/gopub/log"
)

type (
	DB          = sql.DB
	Tx          = sql.Tx
	TxOptions   = sql.TxOptions
	Stmt        = sql.Stmt
	Row         = sql.Row
	Rows        = sql.Rows
	Conn        = sql.Conn
	Result      = sql.Result
	NullInt64   = sql.NullInt64
	NullTime    = sql.NullTime
	NullBool    = sql.NullBool
	NullFloat64 = sql.NullFloat64
	NullInt32   = sql.NullInt32
	NullString  = sql.NullString
	Scanner     = sql.Scanner
)

var (
	ErrNoRows   = sql.ErrNoRows
	ErrTxDone   = sql.ErrTxDone
	ErrConnDone = sql.ErrConnDone
)

type ColumnScanner interface {
	Scan(dest ...interface{}) error
}

type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func Escape(s string) string {
	s = strings.Replace(s, ",", "\\,", -1)
	s = strings.Replace(s, "(", "\\(", -1)
	s = strings.Replace(s, ")", "\\)", -1)
	s = strings.Replace(s, "\"", "\\\"", -1)
	return s
}

func MustPrepare(db *sql.DB, format string, args ...interface{}) *sql.Stmt {
	stmt, err := db.Prepare(fmt.Sprintf(format, args...))
	if err != nil {
		log.Panicf("Prepare: %+v", err)
	}
	return stmt
}

func ToPlaceholderValue(placeholder string, num int) string {
	var b strings.Builder
	for i := 1; i < num; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%s%d", placeholder, i+1))
	}
	return b.String()
}

func ColumnToPlaceholderValue(column, placeholder string) string {
	cols := strings.Split(column, ",")
	return ToPlaceholderValue(placeholder, len(cols))
}
