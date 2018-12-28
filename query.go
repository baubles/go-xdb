package xdb

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type statementType int

const (
	_ statementType = iota
	insertStatement
	deleteStatement
	updateStatement
	selectStatement
)

const (
	and = ") \n and ("
	or  = ") \n or ("
)

const (
	_ = iota
	whereCondition
	havingCondition
)

type query struct {
	_select         []string
	_tables         []string
	_join           []string
	_innerJoin      []string
	_outerJoin      []string
	_leftOuterJoin  []string
	_rightOuterJoin []string
	_where          []string
	_having         []string
	_groupBy        []string
	_orderBy        []string
	_columns        []string
	_values         []string
	_sets           []string
	_limit          []string
	_sql            string
	_args           []interface{}
	_distinct       bool
	_lastCondition  int
	_statementType  statementType

	sqlType statementType
	tokens  []string
	rawSQL  string

	args []interface{}

	querier Querier
	stmt    *sql.Stmt
}

func (q *query) Update(table string) Query {
	q._tables = append(q._tables, table)
	q._statementType = updateStatement
	return q
}

func (q *query) Set(set string) Query {
	q._sets = append(q._sets, set)
	return q
}

func (q *query) DeleteFrom(table string) Query {
	q._tables = append(q._tables, table)
	q._statementType = deleteStatement
	return q
}

func (q *query) InsertInto(table string) Query {
	q._tables = append(q._tables, table)
	q._statementType = insertStatement
	return q
}

func (q *query) Values(values string) Query {
	q._values = append(q._values, values)
	return q
}

func (q *query) Columns(columns string) Query {
	q._columns = append(q._columns, columns)
	return q
}

func (q *query) Select(columns string) Query {
	q._select = append(q._select, columns)
	q._statementType = selectStatement
	return q
}

func (q *query) SelectDistinct(columns string) Query {
	q._distinct = true
	return q.Select(columns)
}

func (q *query) From(tables string) Query {
	q._tables = append(q._tables, tables)
	return q
}

func (q *query) Join(join string) Query {
	q._join = append(q._join, join)
	return q
}

func (q *query) InnerJoin(innerJoin string) Query {
	q._innerJoin = append(q._innerJoin, innerJoin)
	return q
}

func (q *query) LeftJoin(leftOuterJoin string) Query {
	q._leftOuterJoin = append(q._leftOuterJoin, leftOuterJoin)
	return q
}

func (q *query) RightJoin(rightOuterJoin string) Query {
	q._rightOuterJoin = append(q._rightOuterJoin, rightOuterJoin)
	return q
}

func (q *query) OuterJoin(outJoin string) Query {
	q._outerJoin = append(q._outerJoin, outJoin)
	return q
}

func (q *query) Where(where string) Query {
	q._where = append(q._where, where)
	q._lastCondition = whereCondition
	return q
}

func (q *query) Having(having string) Query {
	q._having = append(q._having, having)
	q._lastCondition = havingCondition
	return q
}

func (q *query) And() Query {
	q.addCondition(and)
	return q
}

func (q *query) Or() Query {
	q.addCondition(or)
	return q
}

func (q *query) addCondition(condition string) {
	switch q._lastCondition {
	case whereCondition:
		q._where = append(q._where, condition)
		break
	case havingCondition:
		q._having = append(q._having, condition)
		break
	}
}

func (q *query) GroupBy(having string) Query {
	q._groupBy = append(q._having, having)
	return q
}

func (q *query) OrderBy(orderBy string) Query {
	q._orderBy = append(q._orderBy, orderBy)
	return q
}

func (q *query) Limit(limit string) Query {
	q._limit = append(q._limit, limit)
	return q
}

func (q *query) SQL(sqlString string) Query {
	q._sql = sqlString
	return q
}

func sqlClause(buffer *bytes.Buffer, keyword string, parts []string, openWord string, closeWord string, conjunction string) {
	if len(parts) != 0 {
		if buffer.Len() != 0 {
			buffer.WriteString("\n")
		}
		buffer.WriteString(keyword)
		buffer.WriteString(" ")
		buffer.WriteString(openWord)
		last := "_________"
		for i, v := range parts {
			if i > 0 && v != and && v != or && last != and && last != or {
				buffer.WriteString(conjunction)
			}
			buffer.WriteString(v)
			last = v
		}
		buffer.WriteString(closeWord)
	}
}

func (q *query) selectSQL(buffer *bytes.Buffer) {
	if q._distinct {
		sqlClause(buffer, "SELECT DISTINCT", q._select, "", "", ", ")
	} else {
		sqlClause(buffer, "SELECT", q._select, "", "", ", ")
	}

	sqlClause(buffer, "FROM", q._tables, "", "", ", ")
	sqlClause(buffer, "JOIN", q._join, "", "", "\nJOIN ")
	sqlClause(buffer, "INNER JOIN", q._innerJoin, "", "", "\nINNER JOIN ")
	sqlClause(buffer, "OUTER JOIN", q._outerJoin, "", "", "\nOUTER JOIN ")
	sqlClause(buffer, "LEFT OUTER JOIN", q._leftOuterJoin, "", "", "\nLEFT OUTER JOIN ")
	sqlClause(buffer, "RIGHT OUTER JOIN", q._rightOuterJoin, "", "", "\nRIGHT OUTER JOIN ")
	sqlClause(buffer, "WHERE", q._where, "(", ")", " and ")
	sqlClause(buffer, "GROUP BY", q._groupBy, "", "", ", ")
	sqlClause(buffer, "HAVING", q._having, "(", ")", " and ")
	sqlClause(buffer, "ORDER BY", q._orderBy, "", "", ", ")
	sqlClause(buffer, "LIMIT", q._limit, "", "", "")
}

func (q *query) updateSQL(buffer *bytes.Buffer) {
	sqlClause(buffer, "UPDATE", q._tables, "", "", "")
	sqlClause(buffer, "SET", q._sets, "", "", ", ")
	sqlClause(buffer, "WHERE", q._where, "(", ")", " and ")
}

func (q *query) deleteSQL(buffer *bytes.Buffer) {
	sqlClause(buffer, "DELETE FROM", q._tables, "", "", "")
	sqlClause(buffer, "WHERE", q._where, "(", ")", " and ")
}

func (q *query) insertSQL(buffer *bytes.Buffer) {
	sqlClause(buffer, "INSERT INTO", q._tables, "", "", "")
	sqlClause(buffer, "", q._columns, "(", ")", ", ")
	sqlClause(buffer, "VALUES", q._values, "(", ")", ", ")
}

func (q *query) String() string {
	if q._sql == "" {
		buffer := new(bytes.Buffer)
		switch q._statementType {
		case insertStatement:
			q.insertSQL(buffer)
			break
		case deleteStatement:
			q.deleteSQL(buffer)
			break
		case updateStatement:
			q.updateSQL(buffer)
			break
		case selectStatement:
			q.selectSQL(buffer)
			break
		default:
			return ""
		}
		return buffer.String()
	}
	return q._sql
}

func (q *query) build() {
	if q.stmt == nil && q.rawSQL == "" {
		str := q.String()
		q.sqlType = q._statementType
		q.rawSQL, q.tokens = q.parseToken(str, "${", "}")
	}
}

func (q *query) parseToken(str, openToken, closeToken string) (string, []string) {
	buffer := new(bytes.Buffer)
	var tokens []string
	start := strings.Index(str, openToken)
	for start > -1 {
		if start > 0 && str[start-1] == '\\' {
			buffer.WriteString(str[:start])
			buffer.WriteString(openToken)
			str = str[start+len(openToken):]
		} else {
			offset := strings.Index(str[start:], closeToken)
			if offset == -1 {
				buffer.WriteString(str[:])
				str = ""
			} else {
				buffer.WriteString(str[:start])
				token := str[start+len(openToken) : start+offset]
				tokens = append(tokens, token)
				buffer.WriteString(q.handleToken(token))
				str = str[start+offset+len(closeToken):]
			}
			start = strings.Index(str, openToken)
		}
	}
	buffer.WriteString(str)
	return buffer.String(), tokens
}

func (q *query) handleToken(token string) string {
	return "?"
}

func (q *query) Prepare() error {
	q.build()
	var err error
	q.stmt, err = q.querier.Prepare(q.rawSQL)
	return err
}

func (q *query) Close() error {
	if q.stmt != nil {
		err := q.stmt.Close()
		if err == nil {
			q.stmt = nil
		}
		return err
	}
	return nil
}

func (q *query) Args(args ...interface{}) Query {
	q.args = args
	return q
}

func (q *query) ReflectArgs(reflectArgs interface{}) Query {
	q.build()
	var args []interface{}
	if reflectArgs != nil {
		switch reflect.ValueOf(reflectArgs).Kind() {
		case reflect.Array, reflect.Slice:
			args = reflectArgs.([]interface{})
		default:
			args = make([]interface{}, 0, len(q.tokens))
			for _, token := range q.tokens {
				if val, ok := reflectGetValue(reflectArgs, token); ok {
					args = append(args, val)
				} else {
					args = append(args, nil)
				}
			}
		}
	}
	q.args = args
	return q
}

func (q *query) exec() (sql.Result, error) {
	if q.stmt != nil {
		log(q.rawSQL, q.args...)
		return q.stmt.Exec(q.args...)
	}
	q.build()
	log(q.rawSQL, q.args...)
	return q.querier.Exec(q.rawSQL, q.args...)
}

func (q *query) Exec() (sql.Result, error) {
	return q.exec()
}

func (q *query) rows() (*sql.Rows, error) {
	if q.stmt != nil {
		log(q.rawSQL, q.args...)
		return q.stmt.Query(q.args...)
	}
	q.build()
	log(q.rawSQL, q.args...)
	return q.querier.Query(q.rawSQL, q.args...)
}

func (q *query) List(column string) ([]Value, error) {
	sqlRows, err := q.rows()
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	columns, err := sqlRows.Columns()
	if err != nil {
		return nil, err
	}

	var index = -1
	for i, col := range columns {
		if col == column {
			index = i
			break
		}
	}

	if index == -1 {
		return nil, fmt.Errorf("column %s not exist", column)
	}

	var values = []Value{}
	for i := 0; sqlRows.Next(); i++ {
		colVals, err := scanRow(sqlRows, len(columns))
		if err != nil {
			return nil, err
		}
		values = append(values, Value(colVals[index]))
	}

	return values, nil
}

func (q *query) Row() (Row, error) {
	sqlRows, err := q.rows()
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	columns, err := sqlRows.Columns()
	if err != nil {
		return nil, err
	}

	if sqlRows.Next() == false {
		return nil, sql.ErrNoRows
	}

	row := Row{}
	colVals, err := scanRow(sqlRows, len(columns))
	if err != nil {
		return nil, err
	}
	for i, col := range columns {
		row[col] = Value(colVals[i])
	}

	return row, nil

}

func (q *query) Rows() ([]Row, error) {
	sqlRows, err := q.rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return []Row{}, nil
		}
		return nil, err
	}
	defer sqlRows.Close()

	columns, err := sqlRows.Columns()
	if err != nil {
		return nil, err
	}

	var rows = []Row{}
	for i := 0; sqlRows.Next(); i++ {
		row := Row{}
		colVals, err := scanRow(sqlRows, len(columns))
		if err != nil {
			return nil, err
		}
		for i, col := range columns {
			row[col] = Value(colVals[i])
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (q *query) Value() (Value, error) {
	sqlRows, err := q.rows()
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	if sqlRows.Next() == false {
		return nil, sql.ErrNoRows
	}
	colVals, err := scanRow(sqlRows, 1)
	if err != nil {
		return nil, err
	}
	return Value(colVals[0]), nil
}

func (q *query) ReflectRow(row interface{}) error {
	val := reflect.ValueOf(row)
	ind := reflect.Indirect(val)
	typ := ind.Type()
	if val.Kind() != reflect.Ptr || ind.Kind() != reflect.Struct || typ.String() == "time.Time" {
		return errors.New("xdb rows must be ptr struct")
	}

	sqlRows, err := q.rows()
	if err != nil {
		return err
	}
	defer sqlRows.Close()

	columns, err := sqlRows.Columns()
	if err != nil {
		return err
	}

	indexs := getAllColumnFieldIndex(columns, typ)
	if sqlRows.Next() {
		cols, err := scanRow(sqlRows, len(columns))
		if err != nil {
			return err
		}
		for i, index := range indexs {
			if index != nil {
				setFieldValue(ind.FieldByIndex(index), cols[i])
			}
		}
		return nil
	}
	return sql.ErrNoRows
}

func (q *query) ReflectRows(rows interface{}) (int64, error) {
	var num int64
	val := reflect.ValueOf(rows)
	ind := reflect.Indirect(val)
	if val.Kind() != reflect.Ptr || ind.Kind() != reflect.Slice {
		return num, errors.New("xdb rows must be ptr slice")
	}

	elType := ind.Type().Elem()
	if elType.Kind() == reflect.Ptr {
		elType = elType.Elem()
	}

	if elType.Kind() != reflect.Struct || elType.String() == "time.Time" {
		return num, errors.New("xdb rows must be ptr struct")
	}

	sqlRows, err := q.rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return num, err
	}
	defer sqlRows.Close()

	columns, err := sqlRows.Columns()
	if err != nil {
		return num, err
	}

	indexs := getAllColumnFieldIndex(columns, elType)
	for sqlRows.Next() {
		cols, err := scanRow(sqlRows, len(columns))
		if err != nil {
			return int64(0), err
		}
		if ind.Len() <= int(num) {
			elVal := reflect.New(ind.Type().Elem())
			ind = reflect.Append(ind, elVal.Elem())
		}
		elVal := ind.Index(int(num))
		if elVal.Kind() == reflect.Ptr && elVal.IsNil() {
			elVal.Set(reflect.New(elVal.Type().Elem()))
		}
		elVal = reflect.Indirect(elVal)
		for i, index := range indexs {
			if index != nil && len(index) > 0 {
				setFieldValue(elVal.FieldByIndex(index), cols[i])
			}
		}
		num = num + 1
	}

	val.Elem().Set(ind)
	return num, nil
}

func scanRow(rows *sql.Rows, colNum int) ([]sql.RawBytes, error) {
	var (
		vals = make([]sql.RawBytes, colNum)
		args = make([]interface{}, colNum)
	)
	for i := 0; i < len(vals); i++ {
		args[i] = &vals[i]
	}
	err := rows.Scan(args...)
	for i := 0; i < len(vals); i++ {
		if vals[i] != nil {
			b := make(sql.RawBytes, len(vals[i]))
			copy(b, vals[i])
			vals[i] = b
		}
	}
	return vals, err
}

func setFieldValue(val reflect.Value, bytes sql.RawBytes) {
	switch val.Kind() {
	case reflect.Bool:
		var v = false
		if bytes != nil {
			v, _ = strconv.ParseBool(string(bytes))
		}
		val.SetBool(v)
	case reflect.String:
		val.SetString(string(bytes))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var v = int64(0)
		if bytes != nil {
			v, _ = strconv.ParseInt(string(bytes), 10, 64)
		}
		val.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var v = uint64(0)
		if bytes != nil {
			v, _ = strconv.ParseUint(string(bytes), 10, 64)
		}
		val.SetUint(v)
	case reflect.Float32, reflect.Float64:
		var v = float64(0)
		if bytes != nil {
			v, _ = strconv.ParseFloat(string(bytes), 64)
		}
		val.SetFloat(v)
	case reflect.Struct:
		if _, ok := val.Interface().(time.Time); ok {
			str := string(bytes)
			if len(str) >= 19 {
				t, _ := time.Parse("2006-01-02 15:04:05", str[:19])
				val.Set(reflect.ValueOf(t))
			} else if len(str) >= 10 {
				t, _ := time.Parse("2006-01-02", str[:10])
				val.Set(reflect.ValueOf(t))
			}
		}
	case reflect.Interface:
		val.Set(reflect.ValueOf(bytes))
	}
}

func getAllColumnFieldIndex(columns []string, typ reflect.Type) [][]int {
	indexs := make([][]int, len(columns))
	for i, column := range columns {
		indexs[i] = getColumnFieldIndex(column, typ)
	}
	return indexs
}

var timeType = reflect.TypeOf(time.Time{})

func getColumnFieldIndex(column string, typ reflect.Type) []int {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || typ.ConvertibleTo(timeType) {
		return nil
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Anonymous {
			index := getColumnFieldIndex(column, f.Type)
			if index != nil && len(index) > 0 {
				return append([]int{i}, index...)
			}
		}
		if f.Tag.Get(tagName) == column {
			return []int{i}
		}
		if column == f.Name {
			return []int{i}
		}

	}
	return nil
}

func ensureValueFieldIndex(val reflect.Value, index []int) {
	for _, i := range index {
		val = val.Field(i)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				val.Set(reflect.New(val.Type().Elem()))
			}
			val = val.Elem()
		}
	}
}

func reflectGetValue(root interface{}, property string) (interface{}, bool) {
	ok := true
	rootValue := reflect.ValueOf(root)
	if rootValue.Kind() == reflect.Ptr {
		rootValue = rootValue.Elem()
	}
	var result interface{}
	var value reflect.Value
	switch rootValue.Kind() {
	case reflect.Map:
		key := reflect.ValueOf(property)
		value = rootValue.MapIndex(key)
		break
	case reflect.Struct:
		rootTyp := rootValue.Type()
		for i := 0; i < rootTyp.NumField(); i++ {
			typ := rootTyp.Field(i)
			if typ.Anonymous {
				if inf, ok := reflectGetValue(rootValue.Field(i).Interface(), property); ok {
					return inf, ok
				}
			}
			str, ok := typ.Tag.Lookup(tagName)
			if ok && str == property {
				value = rootValue.Field(i)
				break
			}
			if !ok && typ.Name == property {
				value = rootValue.Field(i)
				break
			}
		}
		break
	default:
		panic(fmt.Sprintf("%v is not (map, *map, struct, *struct)", root))
	}

	if value.IsValid() {
		result = value.Interface()
		ok = true
	} else {
		result = nil
		ok = false
	}

	return result, ok
}
