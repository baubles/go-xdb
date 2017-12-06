package dbHelper

import "database/sql"

func log(sql string, args ...interface{}) {
	if LogFunc != nil {
		LogFunc(sql, args...)
	}
}

type dbHelper struct {
	db *sql.DB
}

type txHelper struct {
	tx *sql.Tx
}

// New new helper of database
func New(db *sql.DB) DBHelper {
	return &dbHelper{
		db: db,
	}
}

func (h dbHelper) Begin() (TXHelper, error) {
	tx, err := h.db.Begin()
	if err != nil {
		return NewTXHelper(tx), nil
	}
	return nil, err
}

func (h dbHelper) NewQuery() Query {
	return &query{querier: h.Querier()}
}

func (h dbHelper) Querier() Querier {
	return h.db
}

// NewTXHelper new helper of transaction
func NewTXHelper(tx *sql.Tx) TXHelper {
	return &txHelper{tx: tx}
}

func (h txHelper) NewQuery() Query {
	return &query{querier: h.Querier()}
}

func (h txHelper) Rollback() error {
	return h.tx.Rollback()
}

func (h txHelper) Commit() error {
	return h.tx.Commit()
}

func (h txHelper) Querier() Querier {
	return h.tx
}
