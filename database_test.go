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
	// Get의 응답 아이템의 NI[0]에 수량을 담아 반환한다.
	// (Returns the count in the NI[0] of the response item of Get.)
	// 아래 예제에서는 where를 1개 지정하였으므로 응답 0번의 아이템에만 데이터가 저장된다.
	// (In the following example, since only one where is specified, the data is stored only in the response item 0.)
	// 오류가 없다면 params의 길이와 응답의 길이는 동일하다.
	// (If there is no error, the length of params and the length of the response are the same.)
	param := simpledb.NewGetParam().WithWhere(simpledb.WhereEqual("pk", "data1")).WithCount()
	if res, err := db.Get([]*simpledb.GetParam{param}); err != nil {
		panic(err)
	} else if len(res) != 1 || len(res[0]) != 1 || res[0][0].NI[0] == nil || *res[0][0].NI[0] != 1 {
		panic("count failed")
	}

	// Get
	// 입력된 파라미터 모두를 1개의 트랜잭션에서 읽는다.
	// (Read all input parameters in one transaction.)
	// 트랜잭션은 읽기만 하므로 commit 없이 rollback 된다.
	// (The transaction only reads, so it is rolled back without commit.)
	// 오류가 없다면 params의 길이와 응답의 길이는 동일하다.
	// (If there is no error, the length of params and the length of the response are the same.)
	param = simpledb.NewGetParam().WithWhere(simpledb.WhereEqual("pk", "data1"))
	if res, err := db.Get([]*simpledb.GetParam{param}); err != nil {
		panic(err)
	} else if len(res) != 1 {
		panic("no result")
	} else if len(res[0]) != 1 {
		panic("no docs")
	} else {
		docdata, err := res[0][0].Decode()
		if err != nil {
			panic(err)
		}
		if docdata["n"].(int64) != 2 || docdata["f"].(float64) != 3.4 || docdata["s"].(string) != "world" {
			panic("doc data mismatch")
		}
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
