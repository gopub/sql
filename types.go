package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
)

type BigInt big.Int

var _ driver.Valuer = (*BigInt)(nil)
var _ sql.Scanner = (*BigInt)(nil)

func (i *BigInt) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	var s string
	var ok bool
	s, ok = src.(string)
	if !ok {
		var b []byte
		b, ok = src.([]byte)
		if ok {
			s = string(b)
		}
	}

	if !ok {
		return fmt.Errorf("failed to parse %v into big.Int", src)
	}

	_, ok = (*big.Int)(i).SetString(s, 10)
	if !ok {
		return fmt.Errorf("failed to parse %v into big.Int", src)
	}
	return nil
}

func (i *BigInt) Value() (driver.Value, error) {
	if i == nil {
		return nil, nil
	}
	return (*big.Int)(i).String(), nil
}

func (i *BigInt) Int() *big.Int {
	return (*big.Int)(i)
}
