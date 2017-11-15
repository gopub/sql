package sql

import (
	"bytes"
	"encoding/json"
	"github.com/jinzhu/inflection"
	"github.com/natande/gox"
	"reflect"
	"strings"
)

type tableNaming interface {
	TableName() string
}

func getTableName(record interface{}) string {
	if n, ok := record.(tableNaming); ok {
		return n.TableName()
	}

	return getTableNameByType(reflect.TypeOf(record))
}

func getTableNameBySlice(records interface{}) string {
	typ := reflect.TypeOf(records)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Slice {
		panic("must be a pointer to slice")
	}

	return getTableNameByType(typ.Elem())
}

func getTableNameByType(typ reflect.Type) string {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		panic("not struct: " + typ.String())
	}

	if typ.Implements(_tableNamingType) {
		return reflect.Zero(typ).Interface().(tableNaming).TableName()
	}

	if reflect.PtrTo(typ).Implements(_tableNamingType) {
		// Pointer receiver may be dereferenced during TableName method call
		// New its elem value in order to make pointer non-nil
		return reflect.New(typ).Interface().(tableNaming).TableName()
		//return reflect.Zero(reflect.PtrTo(typ)).Interface().(tableNaming).TableName()
	}

	return inflection.Plural(gox.CamelToSnake(typ.Name()))
}

func isEmpty(jsonData []byte) bool {
	dataStr := string(jsonData)
	return dataStr == "{}" || dataStr == "[]" || dataStr == "null" || dataStr == "NULL"
}

type Table struct {
	exe        executor
	driverName string
	name       string
}

func (t *Table) Insert(record interface{}) error {
	query, values, err := t.prepareInsertQuery(record)
	if err != nil {
		gox.LogError(err)
		return err
	}
	gox.LogDebug(query, values)
	result, err := t.exe.Exec(query, values...)
	if err != nil {
		gox.LogError(err)
		return err
	}
	v := getStructValue(record)
	info := getColumnInfo(v.Type())
	if len(info.aiName) > 0 && v.FieldByIndex(info.nameToIndex[info.aiName]).Int() == 0 {
		id, err := result.LastInsertId()
		if err != nil {
			gox.LogError(err)
			return err
		}
		v.FieldByIndex(info.nameToIndex[info.aiName]).SetInt(id)
	}

	return nil
}

func (t *Table) prepareInsertQuery(record interface{}) (string, []interface{}, error) {
	v := getStructValue(record)
	info := getColumnInfo(v.Type())

	var columns []string
	values := make([]interface{}, 0, len(info.indexes))
	if len(info.aiName) > 0 && v.FieldByIndex(info.nameToIndex[info.aiName]).Int() == 0 {
		columns = info.notAINames
	} else {
		columns = info.names
	}

	for _, name := range columns {
		k := v.FieldByIndex(info.nameToIndex[name]).Interface()
		if gox.IndexOfString(info.jsonNames, name) >= 0 {
			data, err := json.Marshal(k)
			if err != nil {
				return "", nil, err
			}

			if gox.IndexOfString(info.nullableNames, name) >= 0 && isEmpty(data) {
				values = append(values, nil)
			} else {
				values = append(values, data)
			}
		} else {
			if gox.IndexOfString(info.nullableNames, name) >= 0 && k == reflect.Zero(reflect.TypeOf(k)).Interface() {
				values = append(values, nil)
			} else {
				values = append(values, k)
			}
		}
	}

	var buf bytes.Buffer
	buf.WriteString("INSERT INTO ")
	buf.WriteString(t.name)
	buf.WriteString("(")
	buf.WriteString(strings.Join(columns, ", "))
	buf.WriteString(") VALUES (")
	buf.WriteString(strings.Repeat("?, ", len(columns)))
	buf.Truncate(buf.Len() - 2)
	buf.WriteString(")")
	return buf.String(), values, nil
}

func (t *Table) Update(record interface{}) error {
	v := getStructValue(record)
	info := getColumnInfo(v.Type())
	if len(info.pkNames) == 0 {
		panic("no primary key. please use Insert operation")
	}

	var buf bytes.Buffer
	buf.WriteString("UPDATE ")
	buf.WriteString(t.name)
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
	for _, name := range info.notPKNames {
		k := v.FieldByIndex(info.nameToIndex[name]).Interface()
		if gox.IndexOfString(info.jsonNames, name) >= 0 {
			data, err := json.Marshal(k)
			if err != nil {
				return err
			}
			args = append(args, data)
		} else {
			args = append(args, k)
		}
	}

	for _, name := range info.pkNames {
		args = append(args, v.FieldByIndex(info.nameToIndex[name]).Interface())
	}

	query := buf.String()
	gox.LogDebug(query, args)
	_, err := t.exe.Exec(query, args...)
	return err
}

func (t *Table) Save(record interface{}) error {
	switch t.driverName {
	case "mysql":
		return t.mysqlSave(record)
	case "sqlite3":
		return t.sqliteSave(record)
	default:
		panic("Save operation is not supported for driver: " + t.driverName)
	}
}

func (t *Table) mysqlSave(record interface{}) error {
	query, values, err := t.prepareInsertQuery(record)
	if err != nil {
		gox.LogError(err)
		return err
	}

	v := getStructValue(record)
	info := getColumnInfo(v.Type())

	var buf bytes.Buffer
	buf.WriteString(query)
	buf.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, name := range info.notPKNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(name)
		buf.WriteString(" = ?")
		k := v.FieldByIndex(info.nameToIndex[name]).Interface()
		if gox.IndexOfString(info.jsonNames, name) >= 0 {
			data, err := json.Marshal(k)
			if err != nil {
				gox.LogError(err)
				return err
			}
			values = append(values, data)
		} else {
			values = append(values, k)
		}
	}

	query = buf.String()
	gox.LogDebug(query, values)
	result, err := t.exe.Exec(query, values...)
	if len(info.aiName) > 0 && v.FieldByIndex(info.nameToIndex[info.aiName]).Int() == 0 {
		id, err := result.LastInsertId()
		if err != nil {
			gox.LogError(err)
			return err
		}
		v.FieldByIndex(info.nameToIndex[info.aiName]).SetInt(id)
	}
	return err
}

func (t *Table) sqliteSave(record interface{}) error {
	query, values, err := t.prepareInsertQuery(record)
	if err != nil {
		gox.LogError(err)
		return err
	}

	query = strings.Replace(query, "INSERT INTO", "INSERT OR REPLACE INTO", 1)
	v := getStructValue(record)
	info := getColumnInfo(v.Type())
	gox.LogDebug(query, values)
	result, err := t.exe.Exec(query, values...)
	if len(info.aiName) > 0 && v.FieldByIndex(info.nameToIndex[info.aiName]).Int() == 0 {
		id, err := result.LastInsertId()
		if err != nil {
			gox.LogError(err)
			return err
		}
		v.FieldByIndex(info.nameToIndex[info.aiName]).SetInt(id)
	}
	return err
}

func (t *Table) Select(records interface{}, where string, args ...interface{}) error {
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

	fi := getColumnInfo(elemType)

	var buf bytes.Buffer
	buf.WriteString("SELECT ")
	buf.WriteString(strings.Join(fi.names, ", "))
	buf.WriteString(" FROM ")
	buf.WriteString(t.name)
	if len(where) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
	}
	query := buf.String()
	gox.LogDebug(query, args)
	rows, err := t.exe.Query(query, args...)
	if err != nil {
		gox.LogError(err)
		return err
	}
	defer rows.Close()

	if v.IsNil() {
		v.Set(reflect.New(sliceType))
	}
	sliceValue := v.Elem()
	fields := make([]interface{}, len(fi.indexes))
	for rows.Next() {
		ptrToElem := gox.DeepNew(elemType)
		elem := ptrToElem.Elem()
		for i, idx := range fi.indexes {
			if gox.IndexOfString(fi.jsonNames, fi.names[i]) >= 0 {
				var data []byte
				fields[i] = &data
			} else {
				fields[i] = elem.FieldByIndex(idx).Addr().Interface()
			}
		}

		err = rows.Scan(fields...)
		if err != nil {
			gox.LogError(err)
			return err
		}

		for _, name := range fi.jsonNames {
			idx := fi.nameToIndex[name]
			i := gox.IndexOfString(fi.names, name)
			addr := fields[i]
			data := reflect.ValueOf(addr).Elem().Interface()
			err = json.Unmarshal(data.([]byte), elem.FieldByIndex(idx).Addr().Interface())
			if err != nil {
				gox.LogError(err)
				return err
			}
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

func (t *Table) SelectOne(record interface{}, where string, args ...interface{}) error {
	rv := reflect.ValueOf(record)
	if rv.Kind() != reflect.Ptr {
		panic("not pointer to a struct")
	}

	//Store result in ev. If failed, don't change record's value
	ev := gox.DeepNew(rv.Elem().Type()).Elem()
	v := ev
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic("not pointer to a struct")
	}

	info := getColumnInfo(v.Type())

	var buf bytes.Buffer
	buf.WriteString("SELECT ")
	buf.WriteString(strings.Join(info.names, ", "))
	buf.WriteString(" FROM ")
	buf.WriteString(t.name)
	if len(where) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
	}
	query := buf.String()
	gox.LogDebug(query, args)

	fieldAddrs := make([]interface{}, len(info.indexes))
	for i, idx := range info.indexes {
		if gox.IndexOfString(info.jsonNames, info.names[i]) >= 0 {
			var data []byte
			fieldAddrs[i] = &data
		} else {
			fieldAddrs[i] = v.FieldByIndex(idx).Addr().Interface()
		}
	}
	err := t.exe.QueryRow(query, args...).Scan(fieldAddrs...)
	for _, name := range info.jsonNames {
		idx := info.nameToIndex[name]
		i := gox.IndexOfString(info.names, name)
		addr := fieldAddrs[i]
		data := reflect.ValueOf(addr).Elem().Interface()
		err = json.Unmarshal(data.([]byte), v.FieldByIndex(idx).Addr().Interface())
		if err != nil {
			gox.LogError(err)
			return err
		}
	}
	if err == nil {
		rv.Elem().Set(ev)
	}
	return err
}

/*
func (t *Table) QueryRow(query string, args ...interface{}) *Row {
	row := t.exe.QueryRow(query, args...)
	return (*Row)(row)
}

func (t *Table) Query(query string, args ...interface{}) (*Rows, error) {
	rows, err := t.exe.Query(query, args...)
	return (*Rows)(rows), err
}*/

func (t *Table) Delete(where string, args ...interface{}) error {
	if len(where) == 0 {
		panic("where is empty")
	}
	var buf bytes.Buffer
	buf.WriteString("DELETE FROM ")
	buf.WriteString(t.name)
	buf.WriteString(" WHERE ")
	buf.WriteString(where)

	query := buf.String()
	gox.LogDebug(query, args)
	_, err := t.exe.Exec(query, args...)
	if err != nil {
		gox.LogError(err)
	}
	return err
}

func (t *Table) Count(where string, args ...interface{}) (int, error) {
	var buf bytes.Buffer
	buf.WriteString("SELECT COUNT(*) FROM ")
	buf.WriteString(t.name)
	if len(where) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
	}
	query := buf.String()
	gox.LogDebug(query, args)

	var count int
	err := t.exe.QueryRow(query, args...).Scan(&count)
	if err != nil {
		gox.LogError(err)
		return 0, err
	}

	return count, nil
}
