package mdb

import (
	"database/sql"
	"log"
	"time"

	"github.com/mattn/go-sqlite3"
)

// represent db row
type EmailEntry struct {
	Id          int64
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

func TryCreate(db *sql.DB) {
	// bad DB design; with SERIAL and TIMESTAMP from postgresql would be much better
	_, err := db.Exec(`
		CREATE TABLE emails (
			id 				INTEGER PRIMARY KEY,
			email 			TEXT UNIQUE,
			confirmed_at 	INTEGER,
			opt_out			INTEGER
		);
	`)
	if err != nil {
		// casting error type back to sqlite3.Error using syntax err.()
		// we need this to separate error 'table arleady exists'(this thing doens't need to be handled) from others
		if sqlError, ok := err.(sqlite3.Error); ok {
			// error code 1 means 'table already exists'
			if sqlError.Code != 1 {
				log.Fatal(sqlError)
			}
			// handle all other errors
		} else {
			log.Fatal(err)
		}
	}
}

// creating EmailEntry structure from the DB row
func emailEntryFromRow(row *sql.Rows) (*EmailEntry, error) {
	var (
		id          int64
		email       string
		confirmedAt int64
		optOut      bool
	)

	// scan should be in the same order as columns appear in DB
	err := row.Scan(&id, &email, &confirmedAt, &optOut)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	// convert the time to appropriate time structure
	t := time.Unix(confirmedAt, 0)

	return &EmailEntry{
		Id:          id,
		Email:       email,
		ConfirmedAt: &t,
		OptOut:      optOut,
	}, nil
}
