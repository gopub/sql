package sql

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/gopub/log"
)

var _tableNamingType = reflect.TypeOf((*tableNaming)(nil)).Elem()

type DBWrapper struct {
	db         *sql.DB
	driverName string
}

// NewDBWrapper opens database
// dataSourceName's format: username:password@tcp(host:port)/dbName
func NewDBWrapper(driverName, dataSourceName string) (*DBWrapper, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &DBWrapper{
		db:         db,
		driverName: driverName,
	}, nil
}

func (d *DBWrapper) DB() *sql.DB {
	return d.db
}

func (d *DBWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	log.Debug(query, args)
	return d.db.Exec(query, args...)
}

func (d *DBWrapper) MustExec(query string, args ...interface{}) {
	_, err := d.db.Exec(query, args...)
	if err != nil {
		panic(err)
	}
}

func (d *DBWrapper) Begin() (*TxWrapper, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}

	return &TxWrapper{
		tx:         tx,
		driverName: d.driverName,
	}, nil
}

func (d *DBWrapper) Close() error {
	return d.db.Close()
}

func (d *DBWrapper) Table(nameOrRecord interface{}) *Table {
	name, ok := nameOrRecord.(string)
	if !ok {
		name = getTableName(nameOrRecord)
	}

	return &Table{
		exe:        d.db,
		driverName: d.driverName,
		name:       name,
	}
}

func (d *DBWrapper) Insert(record interface{}) error {
	return d.Table(getTableName(record)).Insert(record)
}

func (d *DBWrapper) BatchInsert(values interface{}) error {
	l := reflect.ValueOf(values)
	if l.Kind() != reflect.Slice {
		return errors.New("not slice")
	}

	tx, err := d.Begin()
	for i := 0; i < l.Len(); i++ {
		err = tx.Insert(l.Index(i).Interface())
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (d *DBWrapper) Update(record interface{}) error {
	return d.Table(getTableName(record)).Update(record)
}

func (d *DBWrapper) BatchUpdate(values interface{}) error {
	l := reflect.ValueOf(values)
	if l.Kind() != reflect.Slice {
		return errors.New("not slice")
	}

	tx, err := d.Begin()
	for i := 0; i < l.Len(); i++ {
		err = tx.Update(l.Index(i).Interface())
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (d *DBWrapper) Save(record interface{}) error {
	return d.Table(getTableName(record)).Save(record)
}

func (d *DBWrapper) MultiSave(values interface{}) error {
	l := reflect.ValueOf(values)
	if l.Kind() != reflect.Slice {
		return errors.New("not slice")
	}

	tx, err := d.Begin()
	for i := 0; i < l.Len(); i++ {
		err = tx.Save(l.Index(i).Interface())
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (d *DBWrapper) Select(records interface{}, where string, args ...interface{}) error {
	return d.Table(getTableNameBySlice(records)).Select(records, where, args...)
}

func (d *DBWrapper) SelectOne(record interface{}, where string, args ...interface{}) error {
	return d.Table(getTableName(record)).SelectOne(record, where, args...)
}
