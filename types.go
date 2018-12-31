package sqlx

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"time"
)

type BaseEntity struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type BigInt big.Int

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
		return errors.New(fmt.Sprintf("failed to parse %v into big.Int", src))
	}

	_, ok = (*big.Int)(i).SetString(s, 10)
	if !ok {
		return errors.New(fmt.Sprintf("failed to parse %v into big.Int", src))
	}
	return nil
}

func (i *BigInt) Value() (driver.Value, error) {
	if i == nil {
		return nil, nil
	}
	return (*big.Int)(i).String(), nil
}

type PhoneNumber struct {
	CountryCode    int    `json:"country_code"`
	NationalNumber int64  `json:"national_number"`
	Extension      string `json:"extension,omitempty" sql:"type:VARCHAR(10)"`
}

func (p *PhoneNumber) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	s := fmt.Sprintf("(%d,%d,%s)", p.CountryCode, p.NationalNumber, p.Extension)
	return s, nil
}
