package sql

import (
	"github.com/natande/goparam"
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
	"json":           {},
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

type columnInfo struct {
	indexes     []fieldIndex //indexes of fields without tag db:"-"
	names       []string     //column names
	nameToIndex map[string]fieldIndex

	pkNames   []string //primary key column names
	aiName    string   //auto increment column name
	jsonNames []string

	//for speed
	notPKNames []string
	notAINames []string
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
		tag := strings.TrimSpace(strings.ToLower(f.Tag.Get("sql")))
		if tag == "-" {
			continue
		}

		if f.Name[0] < 'A' || f.Name[0] > 'Z' {
			if len(tag) > 0 {
				panic("sql column must be exported field: " + f.Name)
			}
			continue
		}

		isJSON := strings.Contains(tag, "json")

		if !isJSON && !isSupportType(f.Type) {
			if len(tag) > 0 {
				panic("invalid type: db column " + typ.Name() + ":" + f.Type.String())
			}
			continue
		}

		var name string
		if len(tag) > 0 {
			strs := strings.Split(tag, ",")
			if len(strs) > 0 {
				if _, ok := _sqlKeywords[strs[0]]; !ok && param.MatchPattern(param.PatternVariable, strs[0]) {
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

		if strings.Contains(tag, "primary key") {
			if isJSON {
				panic("json column can't be primary key")
			}
			info.pkNames = append(info.pkNames, name)
		}

		if strings.Contains(tag, "auto_increment") {
			if len(info.aiName) > 0 {
				panic("duplicate auto_increment")
			}

			if !f.Type.ConvertibleTo(_int64Type) {
				panic("not integer: " + f.Type.String())
			}
			info.aiName = name
		}

		info.indexes = append(info.indexes, f.Index)
		info.names = append(info.names, name)
		info.nameToIndex[name] = f.Index
		if isJSON {
			info.jsonNames = append(info.jsonNames, name)
		}
	}

	for _, name := range info.names {
		if gox.IndexOfString(info.pkNames, name) < 0 {
			info.notPKNames = append(info.notPKNames, name)
		}

		if name != info.aiName {
			info.notAINames = append(info.notAINames, name)
		}
	}

	if len(info.aiName) > 0 && (gox.IndexOfString(info.pkNames, info.aiName) != 0 || len(info.pkNames) != 1) {
		panic("auto_increment must be used with primary key")
	}

	return info
}

func isSupportType(typ reflect.Type) bool {
	if typ == nil {
		return false
	}

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
			for i := range subFields {
				subFields[i].Index = append([]int{i}, subFields[i].Index...)
			}
			fields = append(fields, subFields...)
		} else {
			fields = append(fields, f)
		}
	}

	return fields
}
