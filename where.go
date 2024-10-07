package simpledb

import (
	"fmt"
	"strings"
)

type WhereClause struct {
	Clause string
	Params []any
}

func (w *WhereClause) GetClause() string {
	if w == nil {
		return ""
	}
	return "WHERE " + w.Clause
}

func (w *WhereClause) GetParam() []any {
	if w == nil {
		return nil
	}
	return w.Params
}

func (w *WhereClause) Not() *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`NOT (%v)`, w.Clause),
		Params: w.Params,
	}
}

func (w *WhereClause) And(other *WhereClause) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`(%v) AND (%v)`, w.Clause, other.Clause),
		Params: append(w.Params, other.Params...),
	}
}

func (w *WhereClause) Or(other *WhereClause) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`(%v) OR (%v)`, w.Clause, other.Clause),
		Params: append(w.Params, other.Params...),
	}
}

func WhereEqual(field string, param any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" = ?`, field),
		Params: []any{param},
	}
}

func WhereNotEqual(field string, param any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" != ?`, field),
		Params: []any{param},
	}
}

func WhereLess(field string, param any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" < ?`, field),
		Params: []any{param},
	}
}

func WhereLessEqual(field string, param any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" <= ?`, field),
		Params: []any{param},
	}
}

func WhereGreater(field string, param any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" > ?`, field),
		Params: []any{param},
	}
}

func WhereGreaterEqual(field string, param any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" >= ?`, field),
		Params: []any{param},
	}
}

func WhereIsNull(field string) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" ISNULL`, field),
		Params: []any{},
	}
}

func WhereIsNotNull(field string) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" NOTNULL`, field),
		Params: []any{},
	}
}

func WhereIn(field string, param ...any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" IN (%v)`, field, strings.Repeat("?, ", len(param))[:len(param)*3-2]),
		Params: param,
	}
}

func WhereNotIn(field string, param ...any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" NOT IN (%v)`, field, strings.Repeat("?, ", len(param))[:len(param)*3-2]),
		Params: param,
	}
}

func WhereBettween(field string, paramMin, paramMax any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" BETWEEN ? AND ?`, field),
		Params: []any{paramMin, paramMax},
	}
}

func WhereNotBettween(field string, paramMin, paramMax any) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" NOT BETWEEN ? AND ?`, field),
		Params: []any{paramMin, paramMax},
	}
}

func WhereLike(field string, param string) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" LIKE ?`, field),
		Params: []any{param},
	}
}

func WhereNotLike(field string, param string) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" NOT LIKE ?`, field),
		Params: []any{param},
	}
}

func WhereGlob(field string, param string) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" GLOB ?`, field),
		Params: []any{param},
	}
}

func WhereNotGlob(field string, param string) *WhereClause {
	return &WhereClause{
		Clause: fmt.Sprintf(`"%v" NOT GLOB ?`, field),
		Params: []any{param},
	}
}
