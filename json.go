package simpledb

import (
	"encoding/json"
	"io"
	"strings"
)

func JsonEncode(doc any) (string, error) {
	bt, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(bt), nil
}

func MustJsonEncode(doc any) string {
	if s, err := JsonEncode(doc); err != nil {
		panic(err)
	} else {
		return s
	}
}

func JsonDecode(doc string) (map[string]any, error) {
	return JsonDecode2(strings.NewReader(doc))
}

func MustJsonDecode(doc string) map[string]any {
	if m, err := JsonDecode(doc); err != nil {
		panic(err)
	} else {
		return m
	}
}

func JsonDecode2(r io.Reader) (map[string]any, error) {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	docn := make(map[string]any)
	err := decoder.Decode(&docn)
	if err != nil {
		return nil, err
	}
	return convNumInMap(docn), nil
}

func convNumInMap(m map[string]any) map[string]any {
	for k, v := range m {
		switch t := v.(type) {
		case json.Number:
			if n, err := t.Int64(); err == nil {
				m[k] = n
			} else if n, err := t.Float64(); err == nil {
				m[k] = n
			} else {
				m[k] = t.String()
			}
		case []any:
			m[k] = convNumInSlice(t)
		case map[string]any:
			m[k] = convNumInMap(t)
		}
	}
	return m
}

func convNumInSlice(s []any) []any {
	for i, v := range s {
		switch t := v.(type) {
		case json.Number:
			if n, err := t.Int64(); err == nil {
				s[i] = n
			} else if n, err := t.Float64(); err == nil {
				s[i] = n
			} else {
				s[i] = t.String()
			}
		case []any:
			s[i] = convNumInSlice(t)
		case map[string]any:
			s[i] = convNumInMap(t)
		}
	}
	return s
}

func MustJsonDecode2(r io.Reader) map[string]any {
	if m, err := JsonDecode2(r); err != nil {
		panic(err)
	} else {
		return m
	}
}
