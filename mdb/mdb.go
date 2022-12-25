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
	// sqlite provides auto id for PRIMARY KEY ; it doesn't have appropriate date types so using integer type converting date to unix
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

	// convert the time to appropriate time structure (from seconds to time string)
	t := time.Unix(confirmedAt, 0)

	return &EmailEntry{
		Id:          id,
		Email:       email,
		ConfirmedAt: &t,
		OptOut:      optOut,
	}, nil
}

// CRUD implementation

// CREATE
func CreateEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`
		INSERT INTO emails(email, confirmed_at, opt_out)
		VALUES (
			(?, 0, false)
		);
	`, email) // email will be substituted for the question mark
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// READ
func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(`
		SELECT * FROM emails
		WHERE email = ?;
	`, email)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	// unlike db.Exec, db.Query proceeds reading until closed
	defer rows.Close()

	// Next() prepares next row to be read by scan() and returns true if it's existing or false if no rows left
	for rows.Next() {
		return emailEntryFromRow(rows)
	}

	// such an email is not existing
	return nil, nil
}

// UPSERT (INSERT or UPDATE) with hepl of ON CONFLICT target action
func UpdateEmail(db *sql.DB, entry EmailEntry) error {
	// convert time.Time to int64
	t := entry.ConfirmedAt.Unix()

	_, err := db.Exec(`
		INSERT INTO emails(email, confirmed_at, opt_out)
		VALUES (?, ?, ?)
		ON CONFLICT(email) DO UPDATE
		SET
			confirmed_at = ?,
			opt_out = ?;
	`, entry.Email, t, entry.OptOut, t, entry.OptOut)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// DELETE
func DeleteEmail(db *sql.DB, email string) error {
	// In this specific case, deleting an email will be considered as an unsent email and app will send it again, spamming mail boxes. To avoid that behavior, I just update opt_out to true
	_, err := db.Exec(`
		UPDATE emails
		SET opt_out = true
		WHERE email = ?;
	`, email)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// Pagination functionality

// Represents a number of page and number of emails per page
type GetEmailBatchQueryParams struct {
	Page  int
	Count int
}

func GetEmailBatch(db *sql.DB, params GetEmailBatchQueryParams) ([]EmailEntry, error) {
	var empty []EmailEntry

	// Limit number of rows to params.Count and skip the first rows. params.Page - 1 needed to not skip result from the start
	rows, err := db.Query(`
		SELECT * FROM emails
		WHERE opt_out = false
		ORDER BY id ASC
		LIMIT ?
		OFFSET ?
	`, params.Count, (params.Page-1)*params.Count)
	if err != nil {
		log.Println(err)
		return empty, err
	}

	defer rows.Close()

	emails := make([]EmailEntry, 0, params.Count)

	for rows.Next() {
		entry, err := emailEntryFromRow(rows)
		if err != nil {
			return nil, err
		}

		emails = append(emails, *entry)
	}

	return emails, nil
}
