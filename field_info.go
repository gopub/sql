package gosql

import (
	"reflect"
	"strings"
)

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

		name := ft.Name
		if len(strs[0]) > 0 {
			name = strs[0]
		}

		info.indexes = append(info.indexes, i)
		info.names = append(info.names, name)
	}

	return info
}
