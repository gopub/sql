package gosql

import (
	"sync"
	"testing"
)

func TestExecutor_getFields(t *testing.T) {
	e := &executor{
		typeToFieldInfo: sync.Map{},
	}

	obj := struct {
		Name string      `db:"name"`
		nick string      `db:"-"`
		Age  int         `db:"age"`
		Data []byte      `db:"data"`
		Ext  interface{} `db:"-"`
	}{}

	obj.Name = "hello"
	obj.Age = 10
	obj.Data = []byte("world")
	columns, values := e.getFields(obj)
	t.Log(columns, values)
}
