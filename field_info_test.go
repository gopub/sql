package gosql

import (
	"reflect"
	"testing"
)

func Test_getFieldInfo(t *testing.T) {
	user := struct {
		Name string `db:"name"`
		nick string `db:"-"`
		Age  int    `db:"age"`
	}{}

	info := getFieldInfo(reflect.TypeOf(user))
	t.Log(info.indexes, info.names)
}
