package sqlite

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/gopub/conv"
	"github.com/gopub/log"
	"github.com/gopub/sql"
	"github.com/gopub/types"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type KVRepo struct {
	clock Clock
	db    *sql.DB
	mu    sync.RWMutex
}

func NewKVRepo(db *sql.DB, clock Clock) *KVRepo {
	r := &KVRepo{
		clock: clock,
		db:    db,
	}

	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS kv(
k VARCHAR(255) PRIMARY KEY, 
v BLOB NOT NULL,
updated_at BIGINT NOT NULL
)`)
	if err != nil {
		log.Fatalf("Create table: %v", err)
	}
	return r
}

func (r *KVRepo) SaveInt64(key string, val int64) {
	logger := log.With("key", key)
	r.mu.Lock()
	_, err := r.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)",
		key, fmt.Sprint(val), r.clock.Now())
	r.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (r *KVRepo) GetInt64(key string) (int64, error) {
	var v string
	r.mu.RLock()
	err := r.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(&v)
	r.mu.RUnlock()
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, types.ErrNotExist
		}
		return 0, err
	}

	n, err := conv.ToInt64(v)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (r *KVRepo) SaveData(key string, data []byte) {
	logger := log.With("key", key)
	r.mu.Lock()
	_, err := r.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)", key, data, r.clock.Now())
	r.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (r *KVRepo) GetData(key string) ([]byte, error) {
	var v []byte
	r.mu.RLock()
	err := r.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(&v)
	r.mu.RUnlock()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, types.ErrNotExist
		}
		return nil, fmt.Errorf("query: %w", err)
	}
	return v, nil
}

func (r *KVRepo) SaveString(key string, s string) {
	r.SaveData(key, []byte(s))
}

func (r *KVRepo) GetString(key string) (string, error) {
	data, err := r.GetData(key)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *KVRepo) SavePB(key string, msg proto.Message) {
	logger := log.With("key", key)
	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Errorf("Marshal: %v", err)
		return
	}
	r.mu.Lock()
	_, err = r.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)", key, data, r.clock.Now())
	r.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (r *KVRepo) GetPB(key string, msg proto.Message) error {
	var v []byte
	r.mu.RLock()
	err := r.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(&v)
	r.mu.RUnlock()
	if err != nil {
		if err == sql.ErrNoRows {
			return types.ErrNotExist
		}
		return err
	}
	return proto.Unmarshal(v, msg)
}

func (r *KVRepo) SaveJSON(key string, obj interface{}) {
	logger := log.With("key", key)
	r.mu.Lock()
	_, err := r.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)", key, sql.JSON(obj), r.clock.Now())
	r.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (r *KVRepo) GetJSON(key string, ptrToObj interface{}) error {
	r.mu.RLock()
	err := r.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(sql.JSON(ptrToObj))
	r.mu.RUnlock()
	if err == sql.ErrNoRows {
		return types.ErrNotExist
	}
	return err
}
