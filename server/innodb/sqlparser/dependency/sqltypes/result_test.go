// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"github.com/zhukovaskychina/xmysql-server/server/sqlparser/dependency/querypb"
	"reflect"
	"testing"
)

func TestRepair(t *testing.T) {
	fields := []*querypb.Field{{
		Type: Int64,
	}, {
		Type: VarChar,
	}}
	in := Result{
		Rows: [][]Value{
			{testVal(VarBinary, "1"), testVal(VarBinary, "aa")},
			{testVal(VarBinary, "2"), testVal(VarBinary, "bb")},
		},
	}
	want := Result{
		Rows: [][]Value{
			{testVal(Int64, "1"), testVal(VarChar, "aa")},
			{testVal(Int64, "2"), testVal(VarChar, "bb")},
		},
	}
	in.Repair(fields)
	if !reflect.DeepEqual(in, want) {
		t.Errorf("Repair:\n%#v, want\n%#v", in, want)
	}
}

func TestCopy(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{testVal(Int64, "1"), NULL},
			{testVal(Int64, "2"), MakeTrusted(VarChar, nil)},
			{testVal(Int64, "3"), testVal(VarChar, "")},
		},
	}
	want := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{testVal(Int64, "1"), NULL},
			{testVal(Int64, "2"), testVal(VarChar, "")},
			{testVal(Int64, "3"), testVal(VarChar, "")},
		},
	}
	out := in.Copy()
	// Change in so we're sure out got actually copied
	in.Fields[0].Type = VarChar
	in.Rows[0][0] = testVal(VarChar, "aa")
	if !reflect.DeepEqual(out, want) {
		t.Errorf("Copy:\n%#v, want\n%#v", out, want)
	}
}

func TestStripFieldNames(t *testing.T) {
	testcases := []struct {
		name     string
		in       *Result
		expected *Result
	}{{
		name:     "no fields",
		in:       &Result{},
		expected: &Result{},
	}, {
		name: "empty fields",
		in: &Result{
			Fields: []*querypb.Field{},
		},
		expected: &Result{
			Fields: []*querypb.Field{},
		},
	}, {
		name: "no name",
		in: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}, {
		name: "names",
		in: &Result{
			Fields: []*querypb.Field{{
				Name: "field1",
				Type: Int64,
			}, {
				Name: "field2",
				Type: VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}}
	for _, tcase := range testcases {
		inCopy := tcase.in.Copy()
		out := inCopy.StripFieldNames()
		if !reflect.DeepEqual(out, tcase.expected) {
			t.Errorf("StripFieldNames unexpected result for %v: %v", tcase.name, out)
		}
		if len(tcase.in.Fields) > 0 {
			// check the out array is different than the in array.
			if out.Fields[0] == inCopy.Fields[0] {
				t.Errorf("StripFieldNames modified original Field for %v", tcase.name)
			}
		}
		// check we didn't change the original result.
		if !reflect.DeepEqual(tcase.in, inCopy) {
			t.Errorf("StripFieldNames modified original result")
		}
	}
}

func TestAppendResult(t *testing.T) {
	r1 := &Result{
		RowsAffected: 3,
		Rows: [][]Value{
			{testVal(VarBinary, "1"), testVal(VarBinary, "aa")},
		},
	}
	r2 := &Result{
		RowsAffected: 5,
		Rows: [][]Value{
			{testVal(VarBinary, "2"), testVal(VarBinary, "aa2")},
		},
	}
	r1.AppendResult(r2)

	got := r1
	want := &Result{
		RowsAffected: 8,
		Rows: [][]Value{
			{testVal(VarBinary, "1"), testVal(VarBinary, "aa")},
			{testVal(VarBinary, "2"), testVal(VarBinary, "aa2")},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Append:\n%#v, want\n%#v", got, want)
	}
}
