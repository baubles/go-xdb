package dbhelper

import (
	"database/sql"
	"strconv"
)

// Value colment
type Value sql.RawBytes

// Int value to int64
func (v Value) Int() int64 {
	i, _ := strconv.ParseInt(string(v), 10, 64)
	return i
}

func (v Value) String() string {
	return string(v)
}

// Float value to float64
func (v Value) Float() float64 {
	f, _ := strconv.ParseFloat(string(v), 64)
	return f
}

// Bool value to bool
func (v Value) Bool() bool {
	b, _ := strconv.ParseBool(string(v))
	return b
}

// Row row value
type Row map[string]Value

// Get get value of key
func (r Row) Get(key string) Value {
	return r[key]
}

// Set set value of key
func (r Row) Set(key string, v Value) {
	r[key] = v
}

// Has return row exist key
func (r Row) Has(key string) bool {
	_, ok := r[key]
	return ok
}

// Rows row list
type Rows []Row

// Query sql
type Query interface {
	Update(table string) Query
	Set(set string) Query
	DeleteFrom(table string) Query
	InsertInto(table string) Query
	Columns(columns string) Query
	Values(values string) Query
	Select(columns string) Query
	SelectDistinct(columns string) Query
	From(tables string) Query
	Join(join string) Query
	InnerJoin(innerJoin string) Query
	LeftJoin(leftOuterJoin string) Query
	RightJoin(rightOuterJoin string) Query
	OuterJoin(outJoin string) Query
	Where(where string) Query
	Having(having string) Query
	And() Query
	Or() Query
	GroupBy(having string) Query
	OrderBy(orderBy string) Query
	Limit(limit string) Query
	SQL(sqlString string) Query
	String() string

	Prepare() error
	Close() error

	Args(args ...interface{}) Query
	ReflectArgs(args interface{}) Query

	Exec() (sql.Result, error)
	List(columns string) ([]Value, error)
	Value() (Value, error)
	Row() (Row, error)
	Rows() ([]Row, error)
	ReflectRow(row interface{}) error
	ReflectRows(rows interface{}) (int64, error)
}

// LogFunc func print sql log
var LogFunc func(sql string, args ...interface{})

// DB interface
type DB interface {
	NewQuery() Query
}

// Helper qeury helper
type Helper interface {
	NewQuery() Query
	Querier() Querier
}

// DBHelper db hlper
type DBHelper interface {
	Helper
	Begin() (TXHelper, error)
}

// TXHelper tx helper
type TXHelper interface {
	Helper
	Rollback() error
	Commit() error
}

// Querier sql querier
type Querier interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row

	Prepare(query string) (*sql.Stmt, error)
}

const tagName = "col"
