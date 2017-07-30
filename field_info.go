package gosql

import (
	"github.com/natande/gox"
	"reflect"
	"strings"
)

var bytesType = reflect.TypeOf([]byte(nil))

type fieldInfo struct {
	indexes []int
	names   []string
}

func getFieldInfo(typ reflect.Type) *fieldInfo {
	if typ.Kind() != reflect.Struct {
		panic("not struct")
	}

	info := &fieldInfo{}
	for i := 0; i < typ.NumField(); i++ {
		ft := typ.Field(i)
		strs := strings.Split(ft.Tag.Get("db"), ",")
		if strs[0] == "-" {
			continue
		}

		switch ft.Type.Kind() {
		case reflect.Bool, reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.String:
			break
		default:
			if !ft.Type.ConvertibleTo(bytesType) {
				panic("invalid type: db column " + ft.Name + ":" + ft.Type.String())
			}
		}

		var name string
		if len(strs[0]) > 0 {
			name = strs[0]
		} else {
			name = gox.CamelToSnake(ft.Name)
		}

		info.indexes = append(info.indexes, i)
		info.names = append(info.names, name)
	}

	return info
}
