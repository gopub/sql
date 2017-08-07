package gosql

import (
	"github.com/natande/gox"
	"reflect"
	"strings"
	"sync"
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

type columnInfo struct {
	indexes     []int    //indexes of fields without tag db:"-"
	names       []string //column names
	nameToIndex map[string]int

	pkIndexes []int //primary key column index
	pkNames   []string

	aiIndex int

	notPKIndexes []int
	notPKNames   []string

	notAIIndexes []int
	notAINames   []string
}

func getColumnInfo(typ reflect.Type) *columnInfo {
	if i, ok := _typeToColumnInfo.Load(typ); ok {
		return i.(*columnInfo)
	}

	if typ.Kind() != reflect.Struct {
		panic("not struct")
	}

	info := &columnInfo{}
	info.aiIndex = -1
	info.nameToIndex = make(map[string]int, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		ft := typ.Field(i)
		tag := strings.TrimSpace(strings.ToLower(ft.Tag.Get("sql")))
		var name string
		if len(tag) > 0 {
			if tag == "-" {
				continue
			}

			if ft.Name[0] < 'A' || ft.Name[0] > 'Z' {
				panic("sql column must be exported field: " + ft.Name)
			}

			if !isSupportType(ft.Type) {
				panic("invalid type: db column " + typ.Name() + ":" + typ.String())
			}

			if strings.Contains(tag, "primary key") {
				info.pkIndexes = append(info.pkIndexes, i)
			}

			if strings.Contains(tag, "auto_increment") {
				if info.aiIndex >= 0 {
					panic("duplicate auto_increment")
				}

				if !ft.Type.ConvertibleTo(_int64Type) {
					panic("not integer: " + ft.Type.String())
				}
				info.aiIndex = i
			}

			strs := strings.SplitN(tag, " ", 2)
			if len(strs) > 0 {
				if _, found := _sqlKeywords[strs[0]]; !found && gox.IsVariable(strs[0]) {
					name = strs[0]
				}
			}
		}

		if !isSupportType(ft.Type) {
			continue
		}

		if len(name) == 0 {
			name = gox.CamelToSnake(ft.Name)
		}

		info.indexes = append(info.indexes, i)
		info.names = append(info.names, name)
		info.nameToIndex[name] = i
	}

	for i, idx := range info.indexes {
		name := info.names[i]
		if gox.IndexOfInt(info.pkIndexes, idx) < 0 {
			info.notPKIndexes = append(info.notPKIndexes, idx)
			info.notPKNames = append(info.notPKNames, name)
		} else {
			info.pkNames = append(info.pkNames, name)
		}

		if idx != info.aiIndex {
			info.notAIIndexes = append(info.notAIIndexes, idx)
			info.notAINames = append(info.notAINames, name)
		}
	}

	if info.aiIndex >= 0 && (gox.IndexOfInt(info.pkIndexes, info.aiIndex) != 0 || len(info.pkIndexes) != 1) {
		panic("auto_increment must be used with primary key")
	}

	_typeToColumnInfo.Store(typ, info)
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
