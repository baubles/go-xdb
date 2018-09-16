# database query help tools

## New

```golang
dbConn, err := sql.Open("mysql", dsn)
db = xdb.New(dbConn)
```

## select

```golang

type Entity {
    ID   int64 `db:Id`
    Name string
}

type Args {
    ID int64 `db:Id`
}

args := &Args{Id:1}
row := &Entity{}
rows := []*Entity{}

// reflect to struct
err := b.NewQuery().SQL("select Id, Name from table where Id = ${Id}").ReflectArgs(row).ReflectRow(row)
fmt.Println(err, row)

// reflect to struct list
n, err := b.NewQuery().SQL("select Id, Name from table where Id = ${Id}").ReflectArgs(row).ReflectRows(&rows)
fmt.Println(n, err, rows)

//return map[string]Value
mRow, err := b.NewQuery().SQL("select Id, Name from table where Id = ${Id}").ReflectArgs(row).Row()
fmt.Println(err, mRow)

// return []map[string]Value
mRows, err := b.NewQuery().SQL("select Id, Name from table").Rows()
fmt.Println(err, mRows)
```

## insert & update

```golang

row := &Entity{
    Id: 1,
    Name: "hello",
}

result, err := db.NewQuery().SQL("insert into table (Id, Name) values (?, ?)").Args(row.Id, row.Name).Exec()

result, err := db.NewQuery().InsertInto("table").Columns("Id, Name").Values("${Id}, ${Name}").ReflectArgs(row).Exec()

result, err := db.NewQuery().SQL("update table set Name = ${Name} where Id = ${Id}").ReflectArgs(row).Exec()

result, err := db.NewQuery().Update("table").Set("Name = ${Name}").Where("Id = ${Id}").ReflectArgs(row).Exec()
```

## delete

```golang
row := &Entity{
    Id: 1,
}

result, err := db.NewQuery().DeleteFrom("table").Where("Id = ${Id}").ReflectArgs(row).Exec()
result, err := db.NewQuery().SQL("delete from table where Id = ?").Args(row.Id).Exec()
```

## transaction

```golang

tx, err := db.Begin()
...

if ok {
    tx.Commit()
} else {
    tx.Rollback()
}

```

## Prepare

```golang

rows := []*Entity{...}

query := db.NewQuery().InsertInto("table").Columns("Id, Name").Values("${Id}, ${Name}").Prepare()
defer query.Close()

for _, row := range rows {
    query.ReflectArgs(row).Exec()
}

```