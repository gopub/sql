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
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, password, host, port, name)
	if !sslEnabled {
		url = url + "?sslmode=disable"
	}
	return url
}

func Open(dbURL string) *sql.DB {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Panicf("Open postgres %s: %+v", dbURL, err)
	}

	err = db.Ping()
	if err != nil {
		log.Panicf("Ping %s: %+v", dbURL, err)
	}
	return db
}

func LocalConnURL() string {
	u, err := user.Current()
	if err != nil {
		log.Errorf("Get current user: %v", err)
		return ""
	}
	return fmt.Sprintf("postgres:///%s?host=/var/run/postgresql/", u.Username)
}

func OpenLocalDB() *sql.DB {
	return Open(LocalConnURL())
}
