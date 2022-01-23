// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package oracle

import (
	"fmt"
	"strconv"

	"ariga.io/atlas/schema/schemaspec"
	"ariga.io/atlas/schema/schemaspec/schemahcl"
	"ariga.io/atlas/sql/internal/specutil"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlspec"
)

type (
	doc struct {
		Tables  []*sqlspec.Table  `spec:"table"`
		Schemas []*sqlspec.Schema `spec:"schema"`
	}
)

// UnmarshalSpec unmarshals an Atlas DDL document using an unmarshaler into v.
func UnmarshalSpec(data []byte, unmarshaler schemaspec.Unmarshaler, v interface{}) error {
	var d doc
	if err := unmarshaler.UnmarshalSpec(data, &d); err != nil {
		return err
	}
	switch v := v.(type) {
	case *schema.Realm:
		realm, err := Realm(d.Schemas, d.Tables)
		if err != nil {
			return fmt.Errorf("specutil: failed converting to *schema.Realm: %w", err)
		}
		*v = *realm
	case *schema.Schema:
		if len(d.Schemas) != 1 {
			return fmt.Errorf("specutil: expecting document to contain a single schema, got %d", len(d.Schemas))
		}
		conv, err := Schema(d.Schemas[0], d.Tables)
		if err != nil {
			return fmt.Errorf("specutil: failed converting to *schema.Schema: %w", err)
		}
		*v = *conv
	default:
		return fmt.Errorf("specutil: failed unmarshaling spec. %T is not supported", v)
	}
	return nil
}

// MarshalSpec marshals v into an Atlas DDL document using a schemaspec.Marshaler.
func MarshalSpec(v interface{}, marshaler schemaspec.Marshaler) ([]byte, error) {
	var d doc
	switch s := v.(type) {
	case *schema.Schema:
		var err error
		doc, err := schemaSpec(s)
		if err != nil {
			return nil, fmt.Errorf("specutil: failed converting schema to spec: %w", err)
		}
		d.Tables = doc.Tables
		d.Schemas = doc.Schemas
	case *schema.Realm:
		for _, s := range s.Schemas {
			doc, err := schemaSpec(s)
			if err != nil {
				return nil, fmt.Errorf("specutil: failed converting schema to spec: %w", err)
			}
			d.Tables = append(d.Tables, doc.Tables...)
			d.Schemas = append(d.Schemas, doc.Schemas...)
		}
	default:
		return nil, fmt.Errorf("specutil: failed marshaling spec. %T is not supported", v)
	}
	return marshaler.MarshalSpec(&d)
}

// Realm converts the schemas and tables of the doc into a schema.Realm.
func Realm(schemas []*sqlspec.Schema, tables []*sqlspec.Table) (*schema.Realm, error) {
	r := &schema.Realm{}
	for _, schemaSpec := range schemas {
		var (
			schemaTables []*sqlspec.Table
		)
		for _, tableSpec := range tables {
			name, err := specutil.SchemaName(tableSpec.Schema)
			if err != nil {
				return nil, fmt.Errorf("specutil: cannot extract schema name for table %q: %w", tableSpec.Name, err)
			}
			if name == schemaSpec.Name {
				schemaTables = append(schemaTables, tableSpec)
			}
		}
		sch, err := Schema(schemaSpec, schemaTables)
		if err != nil {
			return nil, err
		}
		r.Schemas = append(r.Schemas, sch)
	}
	return r, nil
}

// Schema converts a sqlspec.Schema with its relevant []sqlspec.Tables into a schema.Schema.
func Schema(spec *sqlspec.Schema, tables []*sqlspec.Table) (*schema.Schema, error) {
	sch := &schema.Schema{
		Name: spec.Name,
	}
	m := make(map[*schema.Table]*sqlspec.Table)
	for _, ts := range tables {
		table, err := convertTable(ts, sch)
		if err != nil {
			return nil, err
		}
		sch.Tables = append(sch.Tables, table)
		m[table] = ts
	}
	for _, tbl := range sch.Tables {
		if err := specutil.LinkForeignKeys(tbl, sch, m[tbl]); err != nil {
			return nil, err
		}
	}
	return sch, nil
}

// convertTable converts a sqlspec.Table to a schema.Table. Table conversion is done without converting
// ForeignKeySpecs into ForeignKeys, as the target tables do not necessarily exist in the schema
// at this point. Instead, the linking is done by the convertSchema function.
func convertTable(spec *sqlspec.Table, parent *schema.Schema) (*schema.Table, error) {
	return specutil.Table(spec, parent, convertColumn, specutil.PrimaryKey, specutil.Index, specutil.Check)
}

// convertColumn converts a sqlspec.Column into a schema.Column.
func convertColumn(spec *sqlspec.Column, _ *schema.Table) (*schema.Column, error) {
	if err := fixDefaultQuotes(spec.Default); err != nil {
		return nil, err
	}
	return specutil.Column(spec, convertColumnType)
}

// fixDefaultQuotes fixes the quotes on the Default field to be single quotes
// instead of double quotes.
func fixDefaultQuotes(value schemaspec.Value) error {
	lv, ok := value.(*schemaspec.LiteralValue)
	if !ok {
		return nil
	}
	if sqlx.IsQuoted(lv.V, '"') {
		uq, err := strconv.Unquote(lv.V)
		if err != nil {
			return err
		}
		lv.V = "'" + uq + "'"
	}
	return nil
}

// convertColumnType converts a sqlspec.Column into a concrete Postgres schema.Type.
func convertColumnType(spec *sqlspec.Column) (schema.Type, error) {
	return TypeRegistry.Type(spec.Type, spec.Extra.Attrs)
}

// schemaSpec converts from a concrete Postgres schema to Atlas specification.
func schemaSpec(schem *schema.Schema) (*doc, error) {
	var d doc
	s, tbls, err := specutil.FromSchema(schem, tableSpec)
	if err != nil {
		return nil, err
	}
	d.Schemas = []*sqlspec.Schema{s}
	d.Tables = tbls
	for _, t := range schem.Tables {
		for _, c := range t.Columns {
			_ = c
		}
	}
	return &d, nil
}

// tableSpec converts from a concrete Postgres sqlspec.Table to a schema.Table.
func tableSpec(tab *schema.Table) (*sqlspec.Table, error) {
	return specutil.FromTable(
		tab,
		columnSpec,
		specutil.FromPrimaryKey,
		specutil.FromIndex,
		specutil.FromForeignKey,
		specutil.FromCheck,
	)
}

// columnSpec converts from a concrete Postgres schema.Column into a sqlspec.Column.
func columnSpec(col *schema.Column, _ *schema.Table) (*sqlspec.Column, error) {
	return specutil.FromColumn(col, columnTypeSpec)
}

// columnTypeSpec converts from a concrete Postgres schema.Type into sqlspec.Column Type.
func columnTypeSpec(t schema.Type) (*sqlspec.Column, error) {
	st, err := TypeRegistry.Convert(t)
	if err != nil {
		return nil, err
	}
	return &sqlspec.Column{Type: st}, nil
}

// TypeRegistry contains the supported TypeSpecs for the Postgres driver.
var TypeRegistry = specutil.NewRegistry(
	specutil.WithFormatter(FormatType),
	specutil.WithParser(ParseType),
	specutil.WithSpecs(
		specutil.TypeSpec(TypeVarchar, specutil.SizeTypeAttr(true)),
		specutil.TypeSpec(TypeNVarchar, specutil.SizeTypeAttr(true)),
		specutil.TypeSpec(TypeChar, specutil.SizeTypeAttr(true)),
		specutil.TypeSpec(TypeNChar, specutil.SizeTypeAttr(true)),
		specutil.TypeSpec(TypeRowID),
		specutil.TypeSpec(TypeRaw, specutil.SizeTypeAttr(true)),
		specutil.TypeSpec(TypeFloat),
		specutil.TypeSpec(TypeDouble),
		specutil.TypeSpec(TypeInt),
		specutil.TypeSpec(TypeNumber),
		specutil.TypeSpec(TypeDate),
		specutil.TypeSpec(TypeTimestamp),
		specutil.TypeSpec(TypeTimestampTZ),
		specutil.TypeSpec(TypeTimestampLTZ),
		specutil.TypeSpec(TypeIntervalDS),
		specutil.TypeSpec(TypeIntervalYM),
		specutil.TypeSpec(TypeCLOB),
		specutil.TypeSpec(TypeBLOB),
		specutil.TypeSpec(TypeBFile),
		specutil.TypeSpec(TypeLongVarchar),
		specutil.TypeSpec(TypeLongRaw),
		specutil.TypeSpec(TypeJSON),
	),
)

var (
	hclState = schemahcl.New(schemahcl.WithTypes(TypeRegistry.Specs()))
	// UnmarshalHCL unmarshals an Atlas HCL DDL document into v.
	UnmarshalHCL = schemaspec.UnmarshalerFunc(func(bytes []byte, i interface{}) error {
		return UnmarshalSpec(bytes, hclState, i)
	})
	// MarshalHCL marshals v into an Atlas HCL DDL document.
	MarshalHCL = schemaspec.MarshalerFunc(func(v interface{}) ([]byte, error) {
		return MarshalSpec(v, hclState)
	})
)
