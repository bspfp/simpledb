package simpledb

import "database/sql"

type DbDoc struct {
	PK   string
	Rev  int64
	Data string

	SI [MaxIndex]*string
	NI [MaxIndex]*int64
}

func NewDbDoc(pk string, rev int64, data string) *DbDoc {
	return &DbDoc{
		PK:   pk,
		Rev:  rev,
		Data: data,
	}
}

func (d *DbDoc) StringIndex(index int, value string) {
	d.SI[index] = &value
}

func (d *DbDoc) Int64Index(index int, value int64) {
	d.NI[index] = &value
}

func (d *DbDoc) Scan(rows *sql.Rows) error {
	fields := make([]any, 3+MaxIndex*2)
	fields[0] = &d.PK
	fields[1] = &d.Rev
	fields[2] = &d.Data
	for i := 0; i < MaxIndex; i++ {
		fields[3+i*2] = &d.SI[i]
		fields[4+i*2] = &d.NI[i]
	}

	if err := rows.Scan(fields...); err != nil {
		return err
	}
	return nil
}

func (d *DbDoc) Decode() (map[string]any, error) {
	if d == nil || d.Data == "" {
		return nil, nil
	}

	return JsonDecode(d.Data)
}
