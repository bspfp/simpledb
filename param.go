package simpledb

import "fmt"

type GetParam struct {
	Where   *WhereClause
	Count   bool
	Limit   string
	OrderBy string
}

func NewGetParam() *GetParam {
	return &GetParam{}
}

func (p *GetParam) WithWhere(where *WhereClause) *GetParam {
	p.Where = where
	return p
}

func (p *GetParam) WithCount() *GetParam {
	p.Count = true
	p.Limit = ""
	p.OrderBy = ""
	return p
}

func (p *GetParam) WithLimit(limit int, offset ...int) *GetParam {
	p.Count = false
	p.Limit = fmt.Sprintf("LIMIT %v", limit)
	if len(offset) > 0 {
		p.Limit = fmt.Sprintf("LIMIT %v OFFSET %v", limit, offset[0])
	}
	return p
}

func (p *GetParam) WithOrderBy(field string, acsOrDesc bool) *GetParam {
	p.Count = false
	var orderby string
	if acsOrDesc {
		orderby = fmt.Sprintf(`"%v" ASC`, field)
	} else {
		orderby = fmt.Sprintf(`"%v" DESC`, field)
	}

	if p.OrderBy == "" {
		p.OrderBy = "ORDER BY " + orderby
		return p
	}

	p.OrderBy += "," + orderby
	return p
}

func (p *GetParam) ToSelect() string {
	return fmt.Sprintf("%v %v %v", p.Where.GetClause(), p.OrderBy, p.Limit)
}
