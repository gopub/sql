package gosql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"testing"
	"time"
)

var _testDB *DB

type Product struct {
	ID        int `sql:"primary key,auto_increment"`
	Name      string
	Price     float32
	Text      string `sql:"txt"`
	UpdatedAt int64
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
	//_testDB.MustExec("drop table products")
	_testDB.Exec(`create table if not exists products(
	id int primary key auto_increment,
	name varchar(20) not null,
	price double not null,
	txt varchar(255) not null,
	updated_at bigint not null
	)`)
}

var _testProduct = &Product{
	Name:      "apple",
	Price:     0.1,
	Text:      "nice",
	UpdatedAt: time.Now().Unix(),
}

func TestExecutor_Insert(t *testing.T) {
	t.Log(_testProduct.ID)
	err := _testDB.Insert(_testProduct)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	if _testProduct.ID == 0 {
		t.Fail()
	}
	t.Log(_testProduct.ID)
}

func TestExecutor_Update(t *testing.T) {
	_testProduct.Name = "apples"
	err := _testDB.Update(_testProduct)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}

func TestExecutor_Save(t *testing.T) {
	{
		_testProduct.ID = 30
		_testProduct.Name = "banana"
		err := _testDB.Save(_testProduct)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}

	{
		_testProduct.Name = "orange"
		err := _testDB.Save(_testProduct)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}

}

func TestExecutor_Select(t *testing.T) {
	{
		var items []Product
		err := _testDB.Select(&items, "")
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		t.Log(items)
	}

	{
		var items []*Product
		err := _testDB.Select(&items, "id>?", 1000)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		for _, v := range items {
			t.Log(*v)
		}
	}
}

func TestExecutor_SelectOne(t *testing.T) {
	{
		var p *Product
		err := _testDB.SelectOne(&p, "")
		if err != nil {
			t.Error(err)
			if err != sql.ErrNoRows {
				t.Fail()
			}
		}
		t.Log(*p)
	}

	{
		var p Product
		err := _testDB.SelectOne(&p, "")
		if err != nil {
			t.Error(err)
			if err != sql.ErrNoRows {
				t.Fail()
			}
		}
		t.Log(p)
	}
}
