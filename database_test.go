package simpledb_test

import (
	"bspfp/simpledb"
	"errors"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestExample(t *testing.T) {
	// 데이터베이스 생성 (Create a database)
	db := simpledb.NewDatabase("testdb", "./data")

	// 열기 (Open)
	if err := db.Open(); err != nil {
		panic(err)
	}
	defer db.Close()

	// Insert
	docs := []*simpledb.DbDoc{
		{
			PK:  "data1",
			Rev: 1,
			Data: simpledb.MustJsonEncode(map[string]any{
				"n": 1,
				"f": 2.3,
				"s": "hello",
			}),
		},
	}
	docs[0].StringIndex(0, "h")
	docs[0].Int64Index(0, 1)
	if err := db.Put(docs); err != nil {
		panic(err)
	}

	// Update
	docs[0].Rev += 1
	docs[0].Data = simpledb.MustJsonEncode(map[string]any{
		"n": 2,
		"f": 3.4,
		"s": "world",
	})
	docs[0].StringIndex(0, "w")
	docs[0].Int64Index(0, 1)
	if err := db.Put(docs); err != nil {
		panic(err)
	}

	// Rev 체크 실패
	var docConflictError *simpledb.DocConflictError
	if err := db.Put(docs); err == nil {
		panic("rev check failed")
	} else if errors.As(err, &docConflictError) {
		// 예상되는 오류
	} else {
		panic(err)
	}

	// Count
	if n, err := db.Count(simpledb.WhereEqual("pk", "data1")); err != nil {
		panic(err)
	} else if n != 1 {
		panic("count failed")
	}

	// Get
	param := simpledb.NewGetParam().WithWhere(simpledb.WhereEqual("pk", "data1"))
	docs, err := db.Get(param)
	if err != nil {
		panic(err)
	}
	if len(docs) == 0 {
		panic("no docs")
	}
	docdata, err := docs[0].Decode()
	if err != nil {
		panic(err)
	}
	if docdata["n"].(int64) != 2 || docdata["f"].(float64) != 3.4 || docdata["s"].(string) != "world" {
		panic("doc data mismatch")
	}

	// Delete
	if n, err := db.Delete(simpledb.WhereEqual("pk", "data2")); err != nil {
		panic(err)
	} else if n != 0 {
		panic("delete failed")
	}
	if n, err := db.Delete(nil); err != nil {
		panic(err)
	} else if n != 1 {
		panic("delete failed")
	}
}
