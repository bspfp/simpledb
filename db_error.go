package simpledb

import "fmt"

type DocConflictError struct {
	Doc *DbDoc
}

func (e *DocConflictError) Error() string {
	return fmt.Sprintf("doc conflict: %q, %v", e.Doc.PK, e.Doc.Rev)
}
