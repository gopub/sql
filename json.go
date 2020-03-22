package sql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/gopub/conv"
)

type jsonHolder struct {
	v interface{}
}

var _ driver.Valuer = (*jsonHolder)(nil)
var _ sql.Scanner = (*jsonHolder)(nil)

func (j *jsonHolder) Scan(src interface{}) error {
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

	err = json.Unmarshal(b, j.v)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

func (j *jsonHolder) Value() (driver.Value, error) {
	return json.Marshal(j.v)
}

func JSON(v interface{}) interface{} {
	return &jsonHolder{v: v}
}
