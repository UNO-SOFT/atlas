// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package oracle

import (
	"context"
	"fmt"

	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"

	"golang.org/x/mod/semver"
)

type (
	// Driver represents a DB driver for introspecting database schemas,
	// generating diff between schema elements and apply migrations changes.
	Driver struct {
		conn
		schema.Differ
		schema.Inspector
		migrate.PlanApplier
	}

	// database connection and its information.
	conn struct {
		schema.ExecQuerier
		// System variables that are set on `Open`.
		collate string
		ctype   string
		version string
	}
)

// Open opens a new PostgreSQL driver.
func Open(db schema.ExecQuerier) (*Driver, error) {
	c := conn{ExecQuerier: db}
	rows, err := db.QueryContext(context.Background(), paramsQuery)
	if err != nil {
		return nil, fmt.Errorf("oracle: scanning system variables: %w", err)
	}
	params, err := sqlx.ScanStrings(rows)
	if err != nil {
		return nil, fmt.Errorf("oracle: failed scanning rows: %w", err)
	}
	if len(params) != 3 {
		return nil, fmt.Errorf("oracle: unexpected number of rows: %d", len(params))
	}
	c.collate, c.ctype, c.version = params[0], params[1], params[2]
	if len(c.version) != 6 {
		return nil, fmt.Errorf("oracle: malformed version: %s", c.version)
	}
	c.version = fmt.Sprintf("%s.%s.%s", c.version[:2], c.version[2:4], c.version[4:])
	if semver.Compare("v"+c.version, "v10.0.0") != -1 {
		return nil, fmt.Errorf("oracle: unsupported oracle version: %s", c.version)
	}
	return &Driver{
		conn:        c,
		Differ:      &sqlx.Diff{DiffDriver: &diff{c}},
		Inspector:   &inspect{c},
		PlanApplier: &planApply{c},
	}, nil
}

// Standard column types (and their aliases).
const (
	TypeVarchar      = "varchar2"
	TypeNVarchar     = "nvarchar2"
	TypeChar         = "char"
	TypeNChar        = "nchar"
	TypeRowID        = "rowid"
	TypeRaw          = "raw"
	TypeFloat        = "float"
	TypeDouble       = "double"
	TypeInt          = "int"
	TypeNumber       = "number"
	TypeDate         = "date"
	TypeTimestamp    = "timestamp"
	TypeTimestampTZ  = "timestamp with time zone"
	TypeTimestampLTZ = "timestamp with local time zone"
	TypeIntervalDS   = "interval day second"
	TypeIntervalYM   = "interval year month"
	TypeCLOB         = "clob"
	TypeBLOB         = "blob"
	TypeBFile        = "bfile"
	TypeLongVarchar  = "long"
	TypeLongRaw      = "long raw"
	TypeJSON         = "json"
)
