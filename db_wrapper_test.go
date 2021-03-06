package sql_test

import (
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gopub/sql"
	"github.com/gopub/types"
)

var _testDB *sql.DBWrapper

type User struct {
	ID    types.ID `sql:"primary key"`
	Phone string   `sql:"nullable"`
	Name  string
}

type Content struct {
	Title string `json:"title"`
	Desc  string `json:"desc"`
}

type ProductID struct {
	ID int `sql:"primary key,auto_increment"`
}

type Product struct {
	ProductID
	Name      string
	Price     float32
	Text      Content `sql:"txt,json"`
	UpdatedAt int64
}

type ItemID struct {
	ID int `sql:"primary key,auto_increment"`
}

type Item struct {
	*ItemID
	Name      string
	Price     float32
	Text      *Content `sql:"txt,json"`
	Email     string   `sql:"nullable"`
	UpdatedAt int64
}

func (i Item) TableName() string {
	return "products"
}

func TestMain(m *testing.M) {
	var err error
	_testDB, err = sql.NewDBWrapper("mysql", "root:password@tcp(localhost:3306)/test")
	if err != nil {
		panic(err)
	}
	r := m.Run()
	os.Exit(r)
}

func TestDB_Exec(t *testing.T) {
	_testDB.MustExec("drop table products")
	_testDB.Exec(`CREATE TABLE IF NOT EXISTS products(
	id INT PRIMARY KEY AUTO_INCREMENT,
	name VARCHAR(20) NOT NULL,
	price DOUBLE NOT NULL,
	txt VARCHAR(255) NOT NULL,
	email VARCHAR(255),
	updated_at BIGINT NOT NULL
	)`)

	_testDB.Exec(`CREATE TABLE IF NOT EXISTS users(
    id BIGINT PRIMARY KEY,
	phone      VARCHAR(20) UNIQUE,
    name VARCHAR(20) NOT NULL DEFAULT ''
	)`)
}

var _testProduct = &Product{
	Name:      "apple",
	Price:     0.1,
	Text:      Content{Title: "nice"},
	UpdatedAt: time.Now().Unix(),
}

var _testItem = &Item{
	ItemID:    &ItemID{},
	Name:      "watermelon",
	Price:     0.3,
	Text:      &Content{Title: "good"},
	UpdatedAt: time.Now().Unix(),
}

func TestDB_Insert(t *testing.T) {
	u := &User{
		ID: types.NewID(),
	}

	err := _testDB.Insert(u)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	u = &User{
		ID: types.NewID(),
	}

	err = _testDB.Insert(u)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	u = &User{
		ID:    types.NewID(),
		Phone: types.NewID().Short(),
	}

	err = _testDB.Insert(u)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

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

	err = _testDB.Insert(_testItem)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	if _testItem.ID == 0 {
		t.Fail()
	}
	t.Log(_testItem.ID)

}

func TestExecutor_Update(t *testing.T) {
	_testProduct.Name = "apples"
	err := _testDB.Update(_testProduct)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	_testItem.Name = "pear"
	err = _testDB.Update(_testItem)
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
		//t.Log(items)
	}

	{
		var items []Item
		err := _testDB.Select(&items, "")
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		//t.Log(items)
	}

	{
		var items []*Product
		err := _testDB.Select(&items, "id>?", 1000)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		//for _, v := range items {
		//	t.Log(*v)
		//}
	}

	{
		var items []*Item
		err := _testDB.Select(&items, "id>?", 1000)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		//for _, v := range items {
		//	t.Log(*v)
		//}
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
		} else {
			t.Log(*p)
		}
	}

	{
		var p *Item
		err := _testDB.SelectOne(&p, "")
		if err != nil {
			t.Error(err)
			if err != sql.ErrNoRows {
				t.Fail()
			}
		} else {
			t.Log(*p)
		}
	}

	{
		var p Product
		err := _testDB.SelectOne(&p, "")
		if err != nil {
			t.Error(err)
			if err != sql.ErrNoRows {
				t.Fail()
			}
		} else {
			t.Log(p)
		}
	}

	{
		var p Item
		err := _testDB.SelectOne(&p, "")
		if err != nil {
			t.Error(err)
			if err != sql.ErrNoRows {
				t.Fail()
			}
		} else {
			t.Log(p)
		}
	}
}
