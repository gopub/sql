package sqlite

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gopub/conv"
	"github.com/gopub/log"
	"github.com/gopub/sql"
	"github.com/gopub/types"
)

type Clock interface {
	Now() time.Time
}

type KVStore struct {
	ID       interface{}
	clock    Clock
	db       *sql.DB
	mu       sync.RWMutex
	filename string
}

func NewKVStore(filename string, clock Clock) *KVStore {
	db := Open(filename)
	r := &KVStore{
		clock:    clock,
		db:       db,
		filename: filename,
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

func (s *KVStore) Filename() string {
	return s.filename
}

func (s *KVStore) SaveInt64(key string, val int64) {
	logger := log.With("key", key)
	s.mu.Lock()
	_, err := s.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)",
		key, fmt.Sprint(val), s.clock.Now())
	s.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (s *KVStore) GetInt64(key string) (int64, error) {
	var v string
	s.mu.RLock()
	err := s.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(&v)
	s.mu.RUnlock()
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

func (s *KVStore) SaveData(key string, data []byte) {
	logger := log.With("key", key)
	s.mu.Lock()
	_, err := s.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)", key, data, s.clock.Now())
	s.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (s *KVStore) GetData(key string) ([]byte, error) {
	var v []byte
	s.mu.RLock()
	err := s.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(&v)
	s.mu.RUnlock()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, types.ErrNotExist
		}
		return nil, fmt.Errorf("query: %w", err)
	}
	return v, nil
}

func (s *KVStore) SaveString(key string, s string) {
	s.SaveData(key, []byte(s))
}

func (s *KVStore) GetString(key string) (string, error) {
	data, err := s.GetData(key)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *KVStore) SavePB(key string, msg proto.Message) {
	logger := log.With("key", key)
	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Errorf("Marshal: %v", err)
		return
	}
	s.mu.Lock()
	_, err = s.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)", key, data, s.clock.Now())
	s.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (s *KVStore) GetPB(key string, msg proto.Message) error {
	var v []byte
	s.mu.RLock()
	err := s.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(&v)
	s.mu.RUnlock()
	if err != nil {
		if err == sql.ErrNoRows {
			return types.ErrNotExist
		}
		return err
	}
	return proto.Unmarshal(v, msg)
}

func (s *KVStore) SaveJSON(key string, obj interface{}) {
	logger := log.With("key", key)
	s.mu.Lock()
	_, err := s.db.Exec("REPLACE INTO kv(k,v,updated_at) VALUES(?1,?2,?3)", key, sql.JSON(obj), s.clock.Now())
	s.mu.Unlock()
	if err != nil {
		logger.Errorf("%v", err)
	}
}

func (s *KVStore) GetJSON(key string, ptrToObj interface{}) error {
	s.mu.RLock()
	err := s.db.QueryRow("SELECT v FROM kv WHERE k=?", key).Scan(sql.JSON(ptrToObj))
	s.mu.RUnlock()
	if err == sql.ErrNoRows {
		return types.ErrNotExist
	}
	return err
}

func (s *KVStore) Close() error {
	s.mu.Lock()
	err := s.db.Close()
	s.mu.Unlock()
	return err
}
