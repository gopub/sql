package sql

import (
	"database/sql"
	"reflect"
)

type executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func getStructValue(i interface{}) reflect.Value {
	v := reflect.ValueOf(i)
	if !v.IsValid() {
		panic("invalid")
	}

	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic("not struct: " + v.Kind().String())
	}

	return v
}
