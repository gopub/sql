package sql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/gopub/conv"
	"github.com/gopub/sql/pg"
	"github.com/gopub/types"
	"github.com/shopspring/decimal"
	"math/big"
	"strings"
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

func (i *BigInt) Unwrap() *big.Int {
	return (*big.Int)(i)
}

type PhoneNumber types.PhoneNumber

var _ driver.Valuer = (*PhoneNumber)(nil)
var _ sql.Scanner = (*PhoneNumber)(nil)

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

var _ driver.Valuer = (*FullName)(nil)
var _ sql.Scanner = (*FullName)(nil)

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
		return fmt.Errorf("failed to parse %v into gox.PhoneNumber", src)
	}

	s = s[1 : len(s)-1]
	segments := strings.Split(s, ",")
	if len(segments) != 3 {
		return fmt.Errorf("failed to parse %v into gox.PhoneNumber", src)
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

type M types.M

var _ driver.Valuer = (M)(nil)
var _ sql.Scanner = (*M)(nil)

func (m *M) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	b, err := conv.ToBytes(src)
	if err != nil {
		return fmt.Errorf("parse bytes: %w", err)
	}

	if len(b) == 0 {
		return nil
	}

	err = json.Unmarshal(b, m)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

func (m M) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (m M) Unwrap() types.M {
	return (types.M)(m)
}

type Any types.Any

var _ driver.Valuer = (*Any)(nil)
var _ sql.Scanner = (*Any)(nil)

func (a *Any) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	b, err := conv.ToBytes(src)
	if err != nil {
		return fmt.Errorf("parse bytes: %w", err)
	}

	if len(b) == 0 {
		return nil
	}

	err = json.Unmarshal(b, a)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

func (a *Any) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *Any) Unwrap() *types.Any {
	return (*types.Any)(a)
}

type Money types.Money

var _ driver.Valuer = (*Money)(nil)
var _ sql.Scanner = (*Money)(nil)

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

var _ driver.Valuer = (*Point)(nil)
var _ sql.Scanner = (*Point)(nil)

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
