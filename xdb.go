package xdb

import "database/sql"

func log(sql string, args ...interface{}) {
	if LogFunc != nil {
		LogFunc(sql, args...)
	}
}

type xdb struct {
	db *sql.DB
}

type xtx struct {
	tx *sql.Tx
}

// New db
func New(db *sql.DB) DB {
	return &xdb{
		db: db,
	}
}

func (x xdb) Begin() (TX, error) {
	tx, err := x.db.Begin()
	if err != nil {
		return nil, err
	}
	return NewTX(tx), nil
}

func (x xdb) NewQuery() Query {
	return &query{querier: x.Querier()}
}

func (x xdb) Querier() Querier {
	return x.db
}

// NewTX new transaction
func NewTX(tx *sql.Tx) TX {
	return &xtx{tx: tx}
}

func (x xtx) NewQuery() Query {
	return &query{querier: x.Querier()}
}

func (x xtx) Rollback() error {
	return x.tx.Rollback()
}

func (x xtx) Commit() error {
	return x.tx.Commit()
}

func (x xtx) Querier() Querier {
	return x.tx
}
