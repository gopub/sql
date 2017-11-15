//package sql
//
//
//import (
//	"database/sql"
//	"reflect"
//	"bytes"
//	"strings"
//	"github.com/natande/gox"
//)
//
//type Rows sql.Rows
//
//func (r *Rows) Scan(records interface{}) error {
//	sr := (*sql.Rows)(r)
//	v := reflect.ValueOf(records)
//	if v.Kind() != reflect.Ptr {
//		panic("must be a pointer to slice")
//	}
//
//	if v.IsNil() && !v.CanSet() {
//		panic("cannot be set value")
//	}
//
//	sliceType := v.Type().Elem()
//	if sliceType.Kind() != reflect.Slice {
//		panic("must be a pointer to slice")
//	}
//
//	isPtr := false
//	elemType := sliceType.Elem()
//	if elemType.Kind() == reflect.Ptr {
//		elemType = elemType.Elem()
//		isPtr = true
//	}
//
//	if elemType.Kind() != reflect.Struct {
//		panic("slice element must be a struct or pointer to struct")
//	}
//
//	if v.IsNil() {
//		v.Set(reflect.New(sliceType))
//	}
//
//	sliceValue := v.Elem()
//	columns, err := sr.Columns()
//	if err != nil {
//		return err
//	}
//
//	fi := getColumnInfo(elemType)
//	indexes := make([]fieldIndex, len(columns))
//	for i, c := range columns {
//		if idx, ok := fi.nameToIndex[c]; ok {
//			indexes[i] = idx
//		} else {
//			panic("no field for column: " + c)
//		}
//	}
//	fields := make([]interface{}, len(indexes))
//	for sr.Next() {
//		ptrToElem := reflect.New(elemType)
//		elem := ptrToElem.Elem()
//		for i, idx := range indexes {
//			fields[i] = elem.FieldByIndex(idx)
//		}
//
//		err := sr.Scan(fields...)
//		if err != nil {
//			return err
//		}
//
//		if isPtr {
//			sliceValue = reflect.Append(sliceValue, ptrToElem)
//		} else {
//			sliceValue = reflect.Append(sliceValue, elem)
//		}
//	}
//	v.Elem().Set(sliceValue)
//	return nil
//}
//
//
//type Row sql.Row
//
//func (r *Row) Get(record interface{}) error {
//	rv := reflect.ValueOf(record)
//	if rv.Kind() != reflect.Ptr {
//		panic("not pointer to a struct")
//	}
//
//	ev := reflect.New(rv.Elem().Type()).Elem()
//	v := ev
//	if v.Kind() == reflect.Ptr {
//		if v.IsNil() {
//			v.Set(reflect.New(v.Type().Elem()))
//		}
//		v = v.Elem()
//	}
//
//	if v.Kind() != reflect.Struct {
//		panic("not pointer to a struct")
//	}
//
//	info := getColumnInfo(v.Type())
//	sr := (*sql.Row)(r)
//	columns, err := sr.Scan()
//	if err != nil {
//		return err
//	}
//
//	indexes := make([]int, len(columns))
//	for i, c := range columns {
//		if idx, ok := info.nameToIndex[c]; ok {
//			indexes[i] = idx
//		} else {
//			panic("no field for column: " + c)
//		}
//	}
//	fields := make([]interface{}, len(indexes))
//	for i, idx := range indexes {
//		fields[i] = v.Field(idx).Addr().Interface()
//	}
//	err = sr.Scan(fields...)
//	if err == nil {
//		rv.Elem().Set(ev)
//	}
//	return err
//}
//
