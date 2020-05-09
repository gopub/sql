package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"strings"

	"github.com/gopub/conv"
	"github.com/gopub/sql/pg"
	"github.com/gopub/types"
	"github.com/shopspring/decimal"
)

type BigInt big.Int

var (
	_ driver.Valuer = (*BigInt)(nil)
	_ sql.Scanner   = (*BigInt)(nil)
)

func (i *BigInt) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	s, err := conv.ToString(src)
	if err != nil {
		return fmt.Errorf("cannot parse %v into big.Int", src)
	}

	_, ok := (*big.Int)(i).SetString(s, 10)
	if !ok {
		return fmt.Errorf("cannot parse %v into big.Int", src)
	}
	return nil
}

func (i *BigInt) Value() (driver.Value, error) {
	if i == nil {
		return nil, nil
	}
	return (*big.Int)(i).String(), nil
}

func (i *BigInt) Unwrap() *big.Int {
	return (*big.Int)(i)
}

type PhoneNumber types.PhoneNumber

var (
	_ driver.Valuer = (*PhoneNumber)(nil)
	_ sql.Scanner   = (*PhoneNumber)(nil)
)

func (n *PhoneNumber) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	s, err := conv.ToString(src)
	if err != nil {
		return fmt.Errorf("parse string: %w", err)
	}
	if len(s) == 0 {
		return nil
	}

	fields, err := pg.ParseCompositeFields(s)
	if err != nil {
		return fmt.Errorf("parse composite fields %s: %w", s, err)
	}

	if len(fields) != 3 {
		return fmt.Errorf("parse composite fields %s: got %v", s, fields)
	}

	n.Code, err = conv.ToInt(fields[0])
	if err != nil {
		return fmt.Errorf("parse code %s: %w", fields[0], err)
	}
	n.Number, err = conv.ToInt64(fields[1])
	if err != nil {
		return fmt.Errorf("parse code %s: %w", fields[1], err)
	}
	n.Extension = fields[2]
	return nil
}

func (n *PhoneNumber) Value() (driver.Value, error) {
	if n == nil {
		return nil, nil
	}
	ext := strings.Replace(n.Extension, ",", "\\,", -1)
	s := fmt.Sprintf("(%d,%d,%s)", n.Code, n.Number, ext)
	return s, nil
}

func (n *PhoneNumber) Unwrap() *types.PhoneNumber {
	return (*types.PhoneNumber)(n)
}

type FullName types.FullName

var (
	_ driver.Valuer = (*FullName)(nil)
	_ sql.Scanner   = (*FullName)(nil)
)

// Scan implements sql.Scanner
func (n *FullName) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	s, ok := src.(string)
	if !ok {
		var b []byte
		b, ok = src.([]byte)
		if ok {
			s = string(b)
		}
	}

	if !ok || len(s) < 4 {
		return fmt.Errorf("failed to parse %v into sql.FullName", src)
	}

	s = s[1 : len(s)-1]
	segments := strings.Split(s, ",")
	if len(segments) != 3 {
		return fmt.Errorf("failed to parse %v into sql.FullName", src)
	}

	n.FirstName, n.MiddleName, n.LastName = segments[0], segments[1], segments[2]
	return nil
}

// Value implements driver.Valuer
func (n *FullName) Value() (driver.Value, error) {
	if n == nil {
		return nil, nil
	}
	s := fmt.Sprintf("(%s,%s,%s)", n.FirstName, n.MiddleName, n.LastName)
	return s, nil
}

func (n *FullName) Unwrap() *types.FullName {
	return (*types.FullName)(n)
}

type Money types.Money

var (
	_ driver.Valuer = (*Money)(nil)
	_ sql.Scanner   = (*Money)(nil)
)

func (m *Money) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	s, ok := src.(string)
	if !ok {
		b, ok := src.([]byte)
		if !ok {
			return fmt.Errorf("src is not []byte or string")
		}
		s = string(b)
	}

	if len(s) == 0 {
		return nil
	}

	fields, err := pg.ParseCompositeFields(s)
	if err != nil {
		return fmt.Errorf("parse composite fields %s: %w", s, err)
	}

	if len(fields) != 2 {
		return fmt.Errorf("parse composite fields %s: got %v", s, fields)
	}
	m.Currency = fields[0]
	m.Amount, err = decimal.NewFromString(fields[1])
	if err != nil {
		return fmt.Errorf("parse amount %s: %w", fields[1], err)
	}
	return nil
}

func (m *Money) Value() (driver.Value, error) {
	return fmt.Sprintf("(%s,%s)", m.Currency, m.Amount.String()), nil
}

func (m *Money) Unwrap() *types.Money {
	return (*types.Money)(m)
}

type Point types.Point

var (
	_ driver.Valuer = (*Point)(nil)
	_ sql.Scanner   = (*Point)(nil)
)

func (p *Point) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	var s string
	switch v := src.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return fmt.Errorf("cannot parse %v into string", src)
	}
	if s == "" {
		return nil
	}
	fields, err := pg.ParseCompositeFields(s)
	if err != nil {
		return fmt.Errorf("parse composite fields %s: %w", s, err)
	}
	if len(fields) == 1 {
		fields = strings.Split(fields[0], " ")
	}
	if len(fields) != 2 {
		return fmt.Errorf("parse composite fields %s", s)
	}
	_, err = fmt.Sscanf(fields[0], "%f", &p.X)
	if err != nil {
		return fmt.Errorf("parse x %s: %w", fields[0], err)
	}
	_, err = fmt.Sscanf(fields[1], "%f", &p.Y)
	if err != nil {
		return fmt.Errorf("parse y %s: %w", fields[1], err)
	}
	return nil
}

func (p *Point) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	v := fmt.Sprintf("POINT(%f %f)", p.X, p.Y)
	return v, nil
}

type Place types.Place

var (
	_ driver.Valuer = (*Place)(nil)
	_ sql.Scanner   = (*Place)(nil)
)

func (p *Place) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	var s string
	switch v := src.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return fmt.Errorf("cannot parse %v into string", src)
	}
	if s == "" {
		return nil
	}
	fields, err := pg.ParseCompositeFields(s)
	if err != nil {
		return fmt.Errorf("parse composite fields %s: %w", s, err)
	}
	if len(fields) != 3 {
		return fmt.Errorf("parse composite fields %s", s)
	}
	p.Code = fields[0]
	p.Name = fields[1]
	if len(fields[2]) > 0 {
		p.Location = new(types.Point)
		if err := (*Point)(p.Location).Scan(fields[2]); err != nil {
			return fmt.Errorf("scan place.location: %w", err)
		}
	}
	return nil
}

func (p *Place) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	loc, err := (*Point)(p.Location).Value()
	if err != nil {
		return nil, fmt.Errorf("get location value: %w", err)
	}
	if locStr, ok := loc.(string); ok {
		loc = Escape(locStr)
	}
	s := fmt.Sprintf("(%s,%s,%s)", Escape(p.Code), Escape(p.Name), loc)
	return s, nil
}
