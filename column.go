package gosql

import (
	"github.com/natande/gox"
	"reflect"
	"strings"
)

var _bytesType = reflect.TypeOf([]byte(nil))
var _int64Type = reflect.TypeOf(int64(0))

type columnInfo struct {
	indexes []int    //indexes of fields without tag db:"-"
	names   []string //column names

	pkIndexes []int //primary key column index
	pkNames   []string

	aiIndex int

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
	info.aiIndex = -1
	for i := 0; i < typ.NumField(); i++ {
		ft := typ.Field(i)
		tag := strings.TrimSpace(strings.ToLower(ft.Tag.Get("db")))
		var name string
		if len(tag) > 0 {
			strs := strings.Split(tag, ",")
			if gox.IndexOfString(strs, "-") >= 0 {
				continue
			}

			if len(strs) > 3 {
				panic("only support ${column_name},primary key,autoincrement")
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

			for _, str := range strs {
				str = strings.TrimSpace(str)
				if str == "primary key" {
					info.pkIndexes = append(info.pkIndexes, i)
				} else if str == "auto_increment" {
					if info.aiIndex >= 0 {
						panic("duplicate auto_increment")
					}

					if !ft.Type.ConvertibleTo(_int64Type) {
						panic("not integer: " + ft.Type.String())
					}
					info.aiIndex = i
				} else if gox.IsVariable(str) {
					if len(name) > 0 {
						panic("duplicate column name: " + str)
					}
					name = str
				} else {
					gox.LogWarn("Unknown tag component:", str)
				}
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

		if idx != info.aiIndex {
			info.notAIIndexes = append(info.notAIIndexes, idx)
			info.notAINames = append(info.notAINames, name)
		}
	}

	if info.aiIndex >= 0 && (gox.IndexOfInt(info.pkIndexes, info.aiIndex) != 0 || len(info.pkIndexes) != 1) {
		panic("auto_increment must be used with primary key")
	}

	return info
}
