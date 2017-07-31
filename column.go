package gosql

import (
	"github.com/natande/gox"
	"reflect"
	"strings"
)

var _bytesType = reflect.TypeOf([]byte(nil))

type columnInfo struct {
	indexes []int    //indexes of fields without tag db:"-"
	names   []string //column names

	pkIndexes []int //primary key column index
	pkNames   []string

	aiIndexes []int //autoincrement column index
	aiNames   []string

	notPKIndexes []int
	notPKNames   []string

	notAIIndexes []int
	notAINames   []string
}

func parseColumnInfo(typ reflect.Type) *columnInfo {
	if typ.Kind() != reflect.Struct {
		panic("not struct")
	}

	info := &columnInfo{}
	for i := 0; i < typ.NumField(); i++ {
		ft := typ.Field(i)
		tag := strings.TrimSpace(strings.ToLower(ft.Tag.Get("db")))
		strs := strings.Split(tag, ",")
		if gox.IndexOfString(strs, "-") >= 0 {
			continue
		}

		switch ft.Type.Kind() {
		case reflect.Bool, reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.String:
			break
		default:
			if !ft.Type.ConvertibleTo(_bytesType) {
				panic("invalid type: db column " + ft.Name + ":" + ft.Type.String())
			}
		}

		var name string
		for _, str := range strs {
			str = strings.TrimSpace(str)
			if str == "primary key" {
				info.pkIndexes = append(info.pkIndexes, i)
			} else if str == "autoincrement" {
				info.aiIndexes = append(info.aiIndexes, i)
			} else if gox.IsVariable(str) {
				name = str
			} else {
				gox.LogWarn("Unknown tag component:", str)
			}
		}

		if len(name) == 0 {
			name = gox.CamelToSnake(ft.Name)
		}

		info.indexes = append(info.indexes, i)
		info.names = append(info.names, name)
	}

	for i, idx := range info.indexes {
		name := info.names[i]
		if gox.IndexOfInt(info.pkIndexes, idx) < 0 {
			info.notPKIndexes = append(info.notPKIndexes, idx)
			info.notPKNames = append(info.notPKNames, name)
		} else {
			info.pkNames = append(info.pkNames, name)
		}

		if gox.IndexOfInt(info.aiIndexes, idx) < 0 {
			info.notAIIndexes = append(info.notAIIndexes, idx)
			info.notAINames = append(info.notAINames, name)
		} else {
			info.aiNames = append(info.aiNames, name)
		}
	}

	return info
}
