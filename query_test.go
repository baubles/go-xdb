package dbhelper

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB
var helper DBHelper

func TestMain(m *testing.M) {
	var err error
	db, err = sql.Open("sqlite3", "./test.db")
	LogFunc = func(sql string, args ...interface{}) {
		fmt.Printf("sql: %s, args: %v \n", sql, args)
	}
	if err != nil {
		panic(err)
	}
	defer func() {
		db.Close()
		os.Remove("./test.db")
	}()

	helper = New(db)
	m.Run()

}

func TestQuery(t *testing.T) {
	t.Run("InitTable", _InitTable)
	t.Run("Insert", _TestInsert)
	t.Run("Select", _TestSelect)
	t.Run("Update", _TestUpdate)
	t.Run("Delete", _TestDelete)
}

var _InitTable = func(t *testing.T) {
	if _, err := helper.NewQuery().SQL("CREATE TABLE `user` (`id` INTEGER PRIMARY KEY AUTOINCREMENT,`username` VARCHAR(64) NULL,`departname` VARCHAR(64) NULL,`created` STRING NULL);\n CREATE TABLE `userdeatail` (`user_id` INT(10) NULL, `intro` TEXT NULL, `profile` TEXT NULL, PRIMARY KEY (`user_id`));").Exec(); err != nil {
		t.Fatal("create table fail", err)
	}
}

var _TestInsert = func(t *testing.T) {
	for i := 0; i < 10; i++ {
		var (
			username   = fmt.Sprintf("mingo-%d", i)
			departname = "dev"
			date       = time.Now().String()
		)
		result, err := helper.NewQuery().InsertInto("user").Columns("username, departname, created").Values("?, ?, ?").Args(username, departname, date).Exec()
		if err != nil {
			t.Fatal("insert fail", err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatal("insert fail", err)
		}
		fmt.Println("insert user, id:", id)
	}
}

var _TestSelect = func(t *testing.T) {
	var (
		val  Value
		vals []Value
		err  error
		row  Row
		rows []Row
	)
	val, err = helper.NewQuery().Select("count(*)").From("user").Where("id < ${Id}").ReflectArgs(map[string]interface{}{"Id": 5}).Value()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)

	val, err = helper.NewQuery().Select("count(*)").From("user").Where("id < ?").Args(5).Value()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)

	row, err = helper.NewQuery().Select("*").From("user").Where("id < ?").Args(5).Row()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(row)

	rows, err = helper.NewQuery().Select("*").From("user").Where("id < ${Id}").ReflectArgs(map[string]interface{}{"Id": 5}).Rows()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(rows)

	vals, err = helper.NewQuery().Select("*").From("user").List("username")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(vals)
}

var _TestUpdate = func(t *testing.T) {
	var (
		result sql.Result
		err    error
		affect int64
	)

	result, err = helper.NewQuery().Update("user").Set("created = ${Date}").Where("id > ${ID}").ReflectArgs(struct {
		Date string
		ID   int64
	}{Date: "2017-10-01 00:00:00", ID: 5}).Exec()
	if err != nil {
		t.Fatal(err)
	}
	affect, err = result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(affect)
}

var _TestDelete = func(t *testing.T) {
	var (
		result sql.Result
		err    error
		affect int64
		val    Value
	)

	result, err = helper.NewQuery().DeleteFrom("user").Where("id > ${ID}").ReflectArgs(struct {
		Date string
		ID   int64
	}{Date: "2017-10-01 00:00:00", ID: 5}).Exec()
	if err != nil {
		t.Fatal(err)
	}
	affect, err = result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(affect)

	val, err = helper.NewQuery().Select("count(*)").From("user").Where("id > ?").Args(5).Value()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)
}
