package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
)

// Connecting Postgres by using environment variables.
func newDB() (db *sql.DB, close func() error, err error) {
	var host string
	var port string
	var user string
	var password string
	var dbname string

	if host = os.Getenv("POSTGRES_HOST"); host == "" {
		return nil, nil, errors.New("host is missing")
	}
	if port = os.Getenv("POSTGRES_PORT"); port == "" {
		return nil, nil, errors.New("port is missing")
	}
	if user = os.Getenv("POSTGRES_USER"); user == "" {
		return nil, nil, errors.New("user is missing")
	}
	if password = os.Getenv("POSTGRES_PASSWORD"); password == "" {
		return nil, nil, errors.New("password is missing")
	}
	if dbname = os.Getenv("POSTGRES_DB"); dbname == "" {
		return nil, nil, errors.New("dbname is missing")
	}

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open postgres by %s: %w", dsn, err)
	}
	if err := db.Ping(); err != nil {
		return nil, nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	return db, db.Close, nil
}

func ping(db *sql.DB) error {
	var err error
	for i := 1; i < 5; i++ {
		if err = db.Ping(); err != nil {
			time.Sleep(time.Duration(i * int(time.Second)))
			continue
		}
	}
	if err != nil {
		return err
	}
	return nil
}
