package gosql

import (
	"bytes"
	"database/sql"
	"github.com/natande/gox"
	"reflect"
	"strings"
	"sync"
)

type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type executor struct {
	se              sqlExecutor
	typeToFieldInfo sync.Map //type:*fieldInfo
}

func (e *executor) getFields(i interface{}) ([]string, []reflect.Value) {
	v := reflect.ValueOf(i)
	if !v.IsValid() {
		panic("invalid")
	}

	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic("not struct")
	}

	var info *fieldInfo
	if i, ok := e.typeToFieldInfo.Load(v.Type()); ok {
		info = i.(*fieldInfo)
	} else {
		info = getFieldInfo(v.Type())
		e.typeToFieldInfo.Store(v.Type(), info)
	}

	values := make([]reflect.Value, len(info.indexes))
	for i, idx := range info.indexes {
		values[i] = v.Field(idx)
	}

	return info.names, values
}

func (e *executor) getFieldValues(i interface{}) ([]string, []interface{}) {
	columns, fields := e.getFields(i)
	values := make([]interface{}, len(columns))
	for i, f := range fields {
		values[i] = f.Interface()
	}
	return columns, values
}

func (e *executor) insert(table string, columns []string, values []interface{}) (sql.Result, error) {
	var buf bytes.Buffer
	buf.WriteString("insert into ")
	buf.WriteString(table)
	buf.WriteString("(")
	buf.WriteString(strings.Join(columns, ","))
	buf.WriteString(") values (")
	buf.WriteString(strings.Repeat("?,", len(columns)))
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(")")
	query := buf.String()
	gox.LogInfo(query, values)
	return e.se.Exec(query, values...)
}

func (e *executor) Insert(table string, record interface{}) (sql.Result, error) {
	var columns []string
	var values []interface{}
	if m, ok := record.(map[string]interface{}); ok {
		for k, v := range m {
			columns = append(columns, k)
			values = append(values, v)
		}
	} else {
		columns, values = e.getFieldValues(record)
	}
	return e.insert(table, columns, values)
}

func (e *executor) Update(table string, record interface{}, where string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

func (e *executor) Upsert(table string, record interface{}, where string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

func (e *executor) Select(table string, records interface{}, where string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

func (e *executor) SelectOne(table string, record interface{}, where string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

func (e *executor) Delete(table string, where string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}
