package pg

import (
	"database/sql"
	"fmt"
	"os/user"

	"github.com/gopub/log"
)

func BuildURL(name, host string, port int, user, password string, sslEnabled bool) string {
	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 5432
	}
	url := fmt.Sprintf("%s:%d/%s", host, port, name)
	if user == "" {
		url = "postgres://" + url
	} else {
		if password == "" {
			url = "postgres://" + user + "@" + url
		} else {
			url = "postgres://" + user + ":" + password + "@" + url
		}
	}
	if !sslEnabled {
		url = url + "?sslmode=disable"
	}
	return url
}

func Open(dbURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return db, nil
}

func MustOpen(dbURL string) *sql.DB {
	db, err := Open(dbURL)
	if err != nil {
		log.Panicf("Cannot open %s: %v", dbURL, err)
	}
	return db
}

func LocalConnURL(unixSocket bool) string {
	u, err := user.Current()
	if err != nil {
		log.Errorf("Get current user: %v", err)
		return ""
	}
	if unixSocket {
		return fmt.Sprintf("postgres:///%s?host=/var/run/postgresql/", u.Username)
	}
	return BuildURL(u.Username, "localhost", 5432, u.Username, "", false)
}

func OpenLocalDB() (*sql.DB, error) {
	url := LocalConnURL(false)
	if db, err := Open(LocalConnURL(false)); err == nil {
		log.Debugf("Connected to %s", url)
		return db, nil
	}
	url = LocalConnURL(true)
	db, err := Open(url)
	if err == nil {
		log.Debugf("Connected to %s", url)
	}
	return db, err
}

func MustOpenLocalDB() *sql.DB {
	db, err := OpenLocalDB()
	if err != nil {
		log.Panicf("Cannot open local db: %v", err)
	}
	return db
}
