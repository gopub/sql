package gosql

import (
	_ "github.com/Go-SQL-Driver/MySQL"
	"github.com/natande/gox"
	"os"
	"testing"
	"time"
)

var _testDB *DB

type product struct {
	ID        int    `db:"id"`
	Name      string `db:"name"`
	UpdatedAt int64  `db:"updated_at"`
}

func TestMain(m *testing.M) {
	db := gox.OpenDB("localhost:3306", "root", "7815", "test")
	_testDB = NewDB(db)
	r := m.Run()
	os.Exit(r)
}

func TestDB_Exec(t *testing.T) {
	_testDB.Exec(`create table if not exists products(
	id int primary key,
	name varchar(20) not null,
	updated_at bigint not null
	)`)
}

func TestExecutor_Insert(t *testing.T) {
	p := &product{
		ID:        int(gox.NewID() % 10000),
		Name:      "apple",
		UpdatedAt: time.Now().Unix(),
	}
	_, err := _testDB.Insert("products", p)
	if err != nil {
		t.Error(err)
		t.Failed()
	}
}

func TestExecutor_Select(t *testing.T) {
	{
		var items []product
		err := _testDB.Select("products", &items, "")
		if err != nil {
			t.Error(err)
			t.Failed()
		}
		t.Log(items)
	}

	{
		var items []*product
		err := _testDB.Select("products", &items, "")
		if err != nil {
			t.Error(err)
			t.Failed()
		}
		for _, v := range items {
			t.Log(*v)
		}
	}
}
