package gosql

import (
	"github.com/natande/gox"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

var _bytesType = reflect.TypeOf([]byte(nil))
var _int64Type = reflect.TypeOf(int64(0))
var _typeToColumnInfo = &sync.Map{} //type:*columnInfo
var _sqlKeywords = map[string]struct{}{
	"primary":        {},
	"key":            {},
	"auto_increment": {},
	"insert":         {},
	"create":         {},
	"table":          {},
	"database":       {},
	"select":         {},
	"update":         {},
	"unique":         {},
	"int":            {},
	"bigint":         {},
	"bool":           {},
	"tinyint":        {},
	"double":         {},
	"date":           {},
}

type fieldIndex []int

func (f fieldIndex) DeepEqual(v fieldIndex) bool {
	return reflect.DeepEqual(f, v)
}

func (f fieldIndex) Equal(v fieldIndex) bool {
	s1 := (*reflect.SliceHeader)(unsafe.Pointer(&f))
	s2 := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	return s1.Data == s2.Data
}

func searchFieldIndex(indexes []fieldIndex, index fieldIndex) int {
	for i, idx := range indexes {
		if idx.Equal(index) {
			return i
		}
	}

	return -1
}

type columnInfo struct {
	indexes     []fieldIndex //indexes of fields without tag db:"-"
	names       []string     //column names
	nameToIndex map[string]fieldIndex

	pkIndexes []fieldIndex //primary key column index
	pkNames   []string

	aiIndex fieldIndex

	notPKIndexes []fieldIndex
	notPKNames   []string

	notAIIndexes []fieldIndex
	notAINames   []string
}

func getColumnInfo(typ reflect.Type) *columnInfo {
	if i, ok := _typeToColumnInfo.Load(typ); ok {
		return i.(*columnInfo)
	}

	if typ.Kind() != reflect.Struct {
		panic("not struct")
	}

	info := parseColumnInfo(typ)
	_typeToColumnInfo.Store(typ, info)
	return info
}

func parseColumnInfo(typ reflect.Type) *columnInfo {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil
	}

	info := &columnInfo{}
	info.nameToIndex = make(map[string]fieldIndex, typ.NumField())

	fields := getAllFields(typ)

	for _, f := range fields {
		if !isSupportType(f.Type) {
			continue
		}

		tag := strings.TrimSpace(strings.ToLower(f.Tag.Get("sql")))
		var name string
		if len(tag) > 0 {
			if tag == "-" {
				continue
			}

			if f.Name[0] < 'A' || f.Name[0] > 'Z' {
				panic("sql column must be exported field: " + f.Name)
			}

			if !isSupportType(f.Type) {
				panic("invalid type: db column " + typ.Name() + ":" + typ.String())
			}

			if strings.Contains(tag, "primary key") {
				info.pkIndexes = append(info.pkIndexes, f.Index)
			}

			if strings.Contains(tag, "auto_increment") {
				if len(info.aiIndex) > 0 {
					panic("duplicate auto_increment")
				}

				if !f.Type.ConvertibleTo(_int64Type) {
					panic("not integer: " + f.Type.String())
				}
				info.aiIndex = f.Index
			}

			strs := strings.SplitN(tag, " ", 2)
			if len(strs) > 0 {
				if _, found := _sqlKeywords[strs[0]]; !found && gox.IsVariable(strs[0]) {
					name = strs[0]
				}
			}
		}

		if len(name) == 0 {
			name = gox.CamelToSnake(f.Name)
		}

		if idx, found := info.nameToIndex[name]; found {
			if len(idx) < len(f.Index) {
				continue
			}

			if len(idx) == len(f.Index) {
				panic("duplicate column name:" + name)
			}
		}

		info.indexes = append(info.indexes, f.Index)
		info.names = append(info.names, name)
		info.nameToIndex[name] = f.Index
	}

	for i, idx := range info.indexes {
		name := info.names[i]
		if searchFieldIndex(info.pkIndexes, idx) < 0 {
			info.notPKIndexes = append(info.notPKIndexes, idx)
			info.notPKNames = append(info.notPKNames, name)
		} else {
			info.pkNames = append(info.pkNames, name)
		}

		if !reflect.DeepEqual(idx, info.aiIndex) {
			info.notAIIndexes = append(info.notAIIndexes, idx)
			info.notAINames = append(info.notAINames, name)
		}
	}

	if len(info.aiIndex) >= 0 && (searchFieldIndex(info.pkIndexes, info.aiIndex) != 0 || len(info.pkIndexes) != 1) {
		panic("auto_increment must be used with primary key")
	}

	return info
}

func isSupportType(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Bool, reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.String:
		return true
	default:
		if typ.ConvertibleTo(_bytesType) {
			return true
		}
	}

	return false
}

func getAllFields(typ reflect.Type) []reflect.StructField {
	fields := make([]reflect.StructField, 0)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Anonymous {
			t := f.Type
			if t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			subFields := getAllFields(t)
			for j := range subFields {
				subFields[j].Index = append([]int{i}, subFields[j].Index...)
			}
			fields = append(fields, subFields...)
		} else {
			fields = append(fields, f)
		}
	}

	return fields
}
