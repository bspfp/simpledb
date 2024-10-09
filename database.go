package simpledb

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"strings"

	_ "modernc.org/sqlite"
)

const MaxIndex = 5

var ErrNoItems = errors.New("items is empty")
var ErrGetConflicted = errors.New("failed to get conflicted doc")

type Database struct {
	Name    string
	DataDir string
	Db      *sql.DB
	DirPerm fs.FileMode
}

func NewDatabase(name, datadir string) *Database {
	return &Database{
		Name:    name,
		DataDir: datadir,
		DirPerm: 0755,
	}
}

func (d *Database) Open() error {
	if err := os.MkdirAll(d.DataDir, d.DirPerm); err != nil {
		return err
	}

	dbfile := path.Join(d.DataDir, fmt.Sprintf("%v.db", d.Name))
	db, err := sql.Open("sqlite", dbfile)
	if err != nil {
		return err
	}

	// modernc.org/sqlite 제한으로 동시성 안되기 때문에 1개로 설정
	db.SetMaxOpenConns(1)

	if err := createTable(db, d.Name); err != nil {
		return err
	}

	d.Db = db

	// 실행 테스트
	param := NewGetParam().WithLimit(1)
	if _, err := d.Get([]*GetParam{param}); err != nil {
		return fmt.Errorf("test failed: %w", err)
	}

	return nil
}

func (d *Database) Close() {
	if err := d.vacuum(); err != nil {
		log.Println("vacuum failed", err)
	}
	if err := d.Db.Close(); err != nil {
		log.Println("db close failed", err)
	}
}

func (d *Database) Get(params []*GetParam) ([][]*DbDoc, error) {
	switch len(params) {
	case 0:
		return nil, ErrNoItems
	case 1:
		return d.get(d.Db, params)
	default:
		tx, err := d.Db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := tx.Rollback(); err != nil {
				log.Println("rollback failed", err)
			}
		}()
		return d.get(tx, params)
	}
}

func (d *Database) Put(items []*DbDoc) error {
	switch len(items) {
	case 0:
		return ErrNoItems
	case 1:
		return d.put(d.Db, items)
	default:
		tx, err := d.Db.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				log.Println("rollback failed", err)
			}
		}()
		if err := d.put(tx, items); err != nil {
			return err
		}
		return tx.Commit()
	}
}

func (d *Database) Delete(where *WhereClause) (int64, error) {
	sql := fmt.Sprintf(`DELETE FROM "%v" %v`, d.Name, where.GetClause())
	res, err := d.Db.Exec(sql, where.GetParam()...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// indexListForStatement `, si1, ni1, si2, ni2, ...`
var indexListForStatement string

// indexListForUpsert `, excluded.si1, excluded.ni1, excluded.si2, excluded.ni2, ...`
var indexListForUpsert string

func createTable(db *sql.DB, name string) error {
	var sqlTable strings.Builder
	sqlTable.Grow(4096)
	sqlIndex := make([]string, MaxIndex*2)

	sqlTable.WriteString(`CREATE TABLE IF NOT EXISTS "`)
	sqlTable.WriteString(name)
	sqlTable.WriteString(`" (pk TEXT NOT NULL PRIMARY KEY, rev INTEGER NOT NULL, data TEXT NOT NULL`)
	for i := 0; i < MaxIndex; i++ {
		sqlTable.WriteString(fmt.Sprintf(`, si%v TEXT`, i))
		indexfield := fmt.Sprintf("si%v", i)
		indexname := fmt.Sprintf("idx_%v_%v", name, indexfield)
		sqlIndex[i*2] = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%v" ON "%v" (%v) WHERE %v NOTNULL`, indexname, name, indexfield, indexfield)
		indexListForStatement += fmt.Sprintf(", %v", indexfield)
		indexListForUpsert += fmt.Sprintf(", excluded.%v", indexfield)

		sqlTable.WriteString(fmt.Sprintf(`, ni%v INTEGER`, i))
		indexfield = fmt.Sprintf("ni%v", i)
		indexname = fmt.Sprintf("idx_%v_%v", name, indexfield)
		sqlIndex[i*2+1] = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%v" ON "%v" (%v) WHERE %v NOTNULL`, indexname, name, indexfield, indexfield)
		indexListForStatement += fmt.Sprintf(", %v", indexfield)
		indexListForUpsert += fmt.Sprintf(", excluded.%v", indexfield)
	}
	sqlTable.WriteString(`)`)

	if _, err := db.Exec(sqlTable.String()); err != nil {
		return err
	}

	for _, sql := range sqlIndex {
		if _, err := db.Exec(sql); err != nil {
			return err
		}
	}

	return nil
}

func (d *Database) vacuum() error {
	if _, err := d.Db.Exec("VACUUM"); err != nil {
		return err
	}
	return nil
}

func (d *Database) get(tx dbExecutor, params []*GetParam) ([][]*DbDoc, error) {
	result := make([][]*DbDoc, 0, len(params))
	for _, param := range params {
		var res []*DbDoc
		var err error
		if param.Count {
			res, err = d.getCount(tx, param.Where)
		} else {
			res, err = d.getItems(tx, param)
		}
		if err != nil {
			return result, err
		}
		result = append(result, res)
	}
	return result, nil
}

func (d *Database) getItems(tx dbExecutor, param *GetParam) ([]*DbDoc, error) {
	sql := fmt.Sprintf(`SELECT pk, rev, data %v FROM "%v" %v`, indexListForStatement, d.Name, param.ToSelect())
	rows, err := tx.Query(sql, param.Where.GetParam()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []*DbDoc
	for rows.Next() {
		var doc DbDoc
		if err := doc.Scan(rows); err != nil {
			return nil, err
		}
		res = append(res, &doc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, err
}

func (d *Database) getCount(tx dbExecutor, where *WhereClause) ([]*DbDoc, error) {
	sql := fmt.Sprintf(`SELECT COUNT(*) FROM "%v" %v`, d.Name, where.GetClause())
	var count int64
	if err := tx.QueryRow(sql, where.GetParam()...).Scan(&count); err != nil {
		return nil, err
	}
	doc := &DbDoc{}
	doc.NI[0] = &count
	return []*DbDoc{doc}, nil
}

func (d *Database) put(tx dbExecutor, items []*DbDoc) error {
	var putSql strings.Builder
	putSql.Grow(4096)
	putSql.WriteString(`INSERT INTO "`)
	putSql.WriteString(d.Name)
	putSql.WriteString(`" (pk, rev, data`)
	putSql.WriteString(indexListForStatement)
	putSql.WriteString(`) VALUES (?, ?, ?, `)
	putSql.WriteString(strings.Repeat("?, ", MaxIndex*2)[:MaxIndex*2*3-2])
	putSql.WriteString(") ON CONFLICT DO UPDATE SET (rev, data")
	putSql.WriteString(indexListForStatement)
	putSql.WriteString(`) = (excluded.rev, excluded.data`)
	putSql.WriteString(indexListForUpsert)
	putSql.WriteString(") WHERE rev < excluded.rev")

	stmt, err := tx.Prepare(putSql.String())
	if err != nil {
		return err
	}

	for _, item := range items {
		params := make([]any, 3+MaxIndex*2)
		params[0] = item.PK
		params[1] = item.Rev
		params[2] = item.Data
		for i := 0; i < MaxIndex; i++ {
			params[3+i*2] = item.SI[i]
			params[4+i*2] = item.NI[i]
		}

		res, err := stmt.Exec(params...)
		if err != nil {
			return err
		}

		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			selectSql := fmt.Sprintf(`SELECT pk, rev, data %v FROM "%v" WHERE pk = ?`, indexListForStatement, d.Name)
			rows, err := tx.Query(selectSql, item.PK)
			if err != nil {
				return err
			}
			defer rows.Close()

			if rows.Next() {
				var doc DbDoc
				if err := doc.Scan(rows); err != nil {
					return err
				}
				return &DocConflictError{Doc: &doc}
			}

			if err := rows.Err(); err != nil {
				return err
			}

			return ErrGetConflicted
		}
	}

	return nil
}

type dbExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
}
