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
	exe             sqlExecutor
	typeToFieldInfo sync.Map //type:*columnInfo
	driverName      string
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

func (e *executor) getStructColumnInfo(typ reflect.Type) *columnInfo {
	var info *columnInfo
	if i, ok := e.typeToFieldInfo.Load(typ); ok {
		info = i.(*columnInfo)
	} else {
		info = parseColumnInfo(typ)
		e.typeToFieldInfo.Store(typ, info)
	}
	return info
}

func (e *executor) Insert(table string, record interface{}) error {
	query, values := e.prepareInsertQuery(table, record)
	gox.LogInfo(query, values)
	result, err := e.exe.Exec(query, values...)
	if err != nil {
		return err
	}
	v := getStructValue(record)
	info := e.getStructColumnInfo(v.Type())
	if info.aiIndex >= 0 && v.Field(info.aiIndex).Int() == 0 {
		id, err := result.LastInsertId()
		if err != nil {
			return err
		}
		v.Field(info.aiIndex).SetInt(id)
	}

	return nil
}

func (e *executor) prepareInsertQuery(table string, record interface{}) (string, []interface{}) {
	v := getStructValue(record)
	info := e.getStructColumnInfo(v.Type())

	var columns []string
	values := make([]interface{}, 0, len(info.indexes))
	if info.aiIndex >= 0 && v.Field(info.aiIndex).Int() == 0 {
		columns = info.notAINames
		for _, idx := range info.notAIIndexes {
			values = append(values, v.Field(idx).Interface())
		}
	} else {
		columns = info.names
		for _, idx := range info.indexes {
			values = append(values, v.Field(idx).Interface())
		}
	}

	var buf bytes.Buffer
	buf.WriteString("INSERT INTO ")
	buf.WriteString(table)
	buf.WriteString("(")
	buf.WriteString(strings.Join(columns, ", "))
	buf.WriteString(") VALUES (")
	buf.WriteString(strings.Repeat("?, ", len(columns)))
	buf.Truncate(buf.Len() - 2)
	buf.WriteString(")")
	return buf.String(), values
}

func (e *executor) Update(table string, record interface{}) error {
	v := getStructValue(record)
	info := e.getStructColumnInfo(v.Type())
	if len(info.pkIndexes) == 0 {
		panic("no primary key. please use Insert operation")
	}

	var buf bytes.Buffer
	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")
	for i, c := range info.notPKNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c)
		buf.WriteString(" = ?")
	}

	buf.WriteString(" WHERE ")
	for i, c := range info.pkNames {
		if i > 0 {
			buf.WriteString(" and ")
		}
		buf.WriteString(c)
		buf.WriteString(" = ?")
	}

	args := make([]interface{}, 0, len(info.indexes))
	for _, idx := range info.notPKIndexes {
		args = append(args, v.Field(idx).Interface())
	}

	for _, idx := range info.pkIndexes {
		args = append(args, v.Field(idx).Interface())
	}

	query := buf.String()
	gox.LogInfo(query, args)
	_, err := e.exe.Exec(query, args...)
	return err
}

func (e *executor) Save(table string, record interface{}) error {
	switch e.driverName {
	case "mysql":
		return e.mysqlSave(table, record)
	case "sqlite3":
		return e.sqliteSave(table, record)
	default:
		panic("Save operation is not supported for driver: " + e.driverName)
	}
}

func (e *executor) mysqlSave(table string, record interface{}) error {
	query, values := e.prepareInsertQuery(table, record)
	v := getStructValue(record)
	info := e.getStructColumnInfo(v.Type())

	var buf bytes.Buffer
	buf.WriteString(query)
	buf.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, c := range info.notPKNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c)
		buf.WriteString(" = ?")
		values = append(values, v.Field(info.notPKIndexes[i]).Interface())
	}

	query = buf.String()
	gox.LogInfo(query, values)
	result, err := e.exe.Exec(query, values...)
	if info.aiIndex >= 0 && v.Field(info.aiIndex).Int() == 0 {
		id, err := result.LastInsertId()
		if err != nil {
			return err
		}
		v.Field(info.aiIndex).SetInt(id)
	}
	return err
}

func (e *executor) sqliteSave(table string, record interface{}) error {
	query, values := e.prepareInsertQuery(table, record)
	query = strings.Replace(query, "INSERT INTO", "INSERT OR REPLACE INTO", 1)
	v := getStructValue(record)
	info := e.getStructColumnInfo(v.Type())
	gox.LogInfo(query, values)
	result, err := e.exe.Exec(query, values...)
	if info.aiIndex >= 0 && v.Field(info.aiIndex).Int() == 0 {
		id, err := result.LastInsertId()
		if err != nil {
			return err
		}
		v.Field(info.aiIndex).SetInt(id)
	}
	return err
}

func (e *executor) Select(table string, records interface{}, where string, args ...interface{}) error {
	v := reflect.ValueOf(records)
	if v.Kind() != reflect.Ptr {
		panic("must be a pointer to slice")
	}

	if v.IsNil() && !v.CanSet() {
		panic("cannot be set value")
	}

	sliceType := v.Type().Elem()
	if sliceType.Kind() != reflect.Slice {
		panic("must be a pointer to slice")
	}

	isPtr := false
	elemType := sliceType.Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		isPtr = true
	}

	if elemType.Kind() != reflect.Struct {
		panic("slice element must be a struct or pointer to struct")
	}

	var fi *columnInfo
	if fv, ok := e.typeToFieldInfo.Load(elemType); ok {
		fi = fv.(*columnInfo)
	} else {
		fi = parseColumnInfo(elemType)
		e.typeToFieldInfo.Store(elemType, fi)
	}

	var buf bytes.Buffer
	buf.WriteString("SELECT ")
	buf.WriteString(strings.Join(fi.names, ", "))
	buf.WriteString(" FROM ")
	buf.WriteString(table)
	if len(where) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
	}
	query := buf.String()
	gox.LogInfo(query, args)
	rows, err := e.exe.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if v.IsNil() {
		v.Set(reflect.New(sliceType))
	}
	sliceValue := v.Elem()
	for rows.Next() {
		ptrToElem := reflect.New(elemType)
		elem := ptrToElem.Elem()
		fields := make([]interface{}, len(fi.indexes))
		for i, idx := range fi.indexes {
			fields[i] = elem.Field(idx).Addr().Interface()
		}

		err = rows.Scan(fields...)
		if err != nil {
			return err
		}

		if isPtr {
			sliceValue = reflect.Append(sliceValue, ptrToElem)
		} else {
			sliceValue = reflect.Append(sliceValue, elem)
		}
	}
	v.Elem().Set(sliceValue)
	return nil
}

func (e *executor) SelectOne(table string, record interface{}, where string, args ...interface{}) error {
	rv := reflect.ValueOf(record)
	if rv.Kind() != reflect.Ptr {
		panic("not pointer to a struct")
	}

	ev := reflect.New(rv.Elem().Type()).Elem()
	v := ev
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic("not pointer to a struct")
	}

	info := e.getStructColumnInfo(v.Type())

	var buf bytes.Buffer
	buf.WriteString("SELECT ")
	buf.WriteString(strings.Join(info.names, ", "))
	buf.WriteString(" FROM ")
	buf.WriteString(table)
	if len(where) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
	}
	query := buf.String()
	gox.LogInfo(query, args)

	fieldAddrs := make([]interface{}, len(info.indexes))
	for i, idx := range info.indexes {
		fieldAddrs[i] = v.Field(idx).Addr().Interface()
	}
	err := e.exe.QueryRow(query, args...).Scan(fieldAddrs...)
	if err == nil {
		rv.Elem().Set(ev)
	}
	return err
}

func (e *executor) Delete(table string, where string, args ...interface{}) (sql.Result, error) {
	var buf bytes.Buffer
	buf.WriteString("DELETE FROM ")
	buf.WriteString(table)
	if len(where) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
	}
	query := buf.String()
	gox.LogInfo(query, args)
	return e.exe.Exec(query, args...)
}
