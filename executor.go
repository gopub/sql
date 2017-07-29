package gosql

import (
	"database/sql"
	"reflect"
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

func (e *executor) getFieldInfo(i interface{}) *fieldInfo {
	typ := reflect.TypeOf(i)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if v, ok := e.typeToFieldInfo.Load(typ); ok {
		return v.(*fieldInfo)
	}

	info := getFieldInfo(typ)
	e.typeToFieldInfo.Store(typ, info)
	return info
}

func (e *executor) Insert(table string, record interface{}) (sql.Result, error) {
	return nil, nil
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
