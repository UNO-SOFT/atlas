// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package oracle

import (
	"fmt"
	"strconv"
	"strings"

	"ariga.io/atlas/sql/schema"
)

// FormatType converts schema type to its column form in the database.
// An error is returned if the type cannot be recognized.
func FormatType(t schema.Type) (string, error) {
	var f string
	switch t := t.(type) {
	case *schema.BinaryType:
		if t.Size < 2 {
			f = "raw(1)"
		} else if t.Size > 4000 {
			f = "blob"
		} else {
			f = fmt.Sprintf("%s(%d)", TypeRaw, t.Size)
		}
	case *schema.IntegerType:
		f = TypeInt
	case *schema.StringType:
		if t.Size < 2 {
			f = "char(1)"
		} else if t.Size > 4000 {
			f = "clob"
		} else {
			f = fmt.Sprintf("%s(%d)", TypeVarchar, t.Size)
		}
	case *schema.TimeType:
		switch f = strings.ToLower(t.T); f {
		// TIMESTAMPTZ is accepted as an abbreviation for TIMESTAMP WITH TIME ZONE.
		case TypeTimestampLTZ:
			f = TypeTimestampLTZ
		case TypeTimestampTZ:
			f = TypeTimestampTZ
		case TypeTimestamp:
			f = TypeTimestamp
		case TypeDate:
			f = TypeDate
		}
	case *schema.FloatType:
		switch f = strings.ToLower(t.T); f {
		case TypeFloat:
			f = TypeFloat
		case TypeDouble:
			f = TypeDouble
		}
	case *schema.DecimalType:
		f = TypeNumber
		switch p, s := t.Precision, t.Scale; {
		case p == 0 && s == 0:
		case s < 0:
			return "", fmt.Errorf("oracle: decimal type must have scale >= 0: %d", s)
		case p == 0 && s > 0:
			return "", fmt.Errorf("oracle: decimal type must have precision between 1 and 1000: %d", p)
		case s == 0:
			f = fmt.Sprintf("%s(%d)", f, p)
		default:
			f = fmt.Sprintf("%s(%d,%d)", f, p, s)
		}
	case *schema.JSONType:
		f = strings.ToLower(t.T)
	case *schema.UnsupportedType:
		return "", fmt.Errorf("oracle: unsupported type: %q", t.T)
	default:
		return "", fmt.Errorf("oracle: invalid schema type: %T", t)
	}
	return f, nil
}

// mustFormat calls to FormatType and panics in case of error.
func mustFormat(t schema.Type) string {
	s, err := FormatType(t)
	if err != nil {
		panic(err)
	}
	return s
}

// ParseType returns the schema.Type value represented by the given raw type.
// The raw value is expected to follow the format in PostgreSQL information schema
// or as an input for the CREATE TABLE statement.
func ParseType(typ string) (schema.Type, error) {
	d, err := parseColumn(typ)
	if err != nil {
		return nil, err
	}
	t := columnType(d)
	// If the type is unknown (to us), we fallback to user-defined but expect
	// to improve this in future versions by ensuring this against the database.
	if ut, ok := t.(*schema.UnsupportedType); ok {
		t = &UserDefinedType{T: ut.T}
	}
	return t, nil
}

// columnDesc represents a column descriptor.
type columnDesc struct {
	typ       string
	size      int64
	udt       string
	precision int64
	scale     int64
	typtype   string
	typid     int64
	parts     []string
}

func parseColumn(s string) (*columnDesc, error) {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '(' || r == ')' || r == ' ' || r == ','
	})
	var (
		err error
		c   = &columnDesc{
			typ:   parts[0],
			parts: parts,
		}
	)
	switch c.parts[0] {
	case TypeVarchar, TypeNVarchar, TypeChar, TypeNChar:
		if err := parseCharParts(c.parts, c); err != nil {
			return nil, err
		}
	case TypeNumber:
		if len(parts) > 1 {
			c.precision, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("oracle: parse precision %q: %w", parts[1], err)
			}
		}
		if len(parts) > 2 {
			c.scale, err = strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("oracle: parse scale %q: %w", parts[1], err)
			}
		}
	case TypeDouble:
		c.precision = 53
	case TypeInt:
		c.precision = 32
	case TypeFloat:
		c.precision = 24
	default:
		c.typ = s
	}
	return c, nil
}

func parseCharParts(parts []string, c *columnDesc) error {
	j := strings.Join(parts, " ")
	switch {
	case strings.HasPrefix(j, TypeVarchar):
		c.typ = TypeVarchar
		parts = parts[1:]
	case strings.HasPrefix(j, TypeNVarchar):
		c.typ = TypeVarchar
		parts = parts[1:]
	case strings.HasPrefix(j, TypeChar):
		c.typ = TypeChar
		parts = parts[2:]
	case strings.HasPrefix(j, TypeNChar):
		c.typ = TypeNChar
		parts = parts[2:]
	default:
		parts = parts[1:]
	}
	if len(parts) == 0 {
		return nil
	}
	size, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("oracle: parse size %q: %w", parts[1], err)
	}
	c.size = size
	return nil
}
