package gosql

import (
	"database/sql"
	_ "github.com/Go-SQL-Driver/MySQL"
	"github.com/natande/gox"
	"os"
	"testing"
	"time"
)

var _testDB *DB

type product struct {
	ID        int    `db:"id,primary key"`
	Name      string `db:"name"`
	UpdatedAt int64  `db:"updated_at"`
}

type readProduct struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	//UpdatedAt int64  `db:"updated_at"`
}

func TestMain(m *testing.M) {
	var err error
	_testDB, err = Open("mysql", "root:7815@tcp(localhost:3306)/test")
	if err != nil {
		panic(err)
	}
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

var _p = &product{
	ID:        int(gox.NewID() % 10000),
	Name:      "apple",
	UpdatedAt: time.Now().Unix(),
}

func TestExecutor_Insert(t *testing.T) {
	t.Log(_p.ID)
	_, err := _testDB.Insert("products", _p)
	if err != nil {
		t.Error(err)
		t.Failed()
	}
}

func TestExecutor_Update(t *testing.T) {
	_p.Name = "apples"
	_, err := _testDB.Update("products", _p)
	if err != nil {
		t.Error(err)
		t.Failed()
	}
}

func TestExecutor_Save(t *testing.T) {
	{
		_p.ID = 12
		_p.Name = "banana"
		_, err := _testDB.Save("products", _p)
		if err != nil {
			t.Error(err)
			t.Failed()
		}
	}

	{
		_p.Name = "orange"
		_, err := _testDB.Save("products", _p)
		if err != nil {
			t.Error(err)
			t.Failed()
		}
	}

}

func TestExecutor_Select(t *testing.T) {
	{
		var items []readProduct
		err := _testDB.Select("products", &items, "")
		if err != nil {
			t.Error(err)
			t.Failed()
		}
		t.Log(items)
	}

	{
		var items []*readProduct
		err := _testDB.Select("products", &items, "id>?", 1000)
		if err != nil {
			t.Error(err)
			t.Failed()
		}
		for _, v := range items {
			t.Log(*v)
		}
	}
}

func TestExecutor_SelectOne(t *testing.T) {
	{
		var p *readProduct
		err := _testDB.SelectOne("products", &p, "")
		if err != nil {
			t.Error(err)
			if err != sql.ErrNoRows {
				t.Failed()
			}
		}
		t.Log(*p)
	}

	{
		var p readProduct
		err := _testDB.SelectOne("products", &p, "")
		if err != nil {
			t.Error(err)
			if err != sql.ErrNoRows {
				t.Failed()
			}
		}
		t.Log(p)
	}
}
