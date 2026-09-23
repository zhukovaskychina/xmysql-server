package plan

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
)

func TestSpatialConstructorsRoundTripWKTAndSRID(t *testing.T) {
	point, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(1.25 -2.5)", int64(4326)})
	if err != nil {
		t.Fatalf("ST_GeomFromText failed: %v", err)
	}
	if got, err := evalSpatialFunction("ST_SRID", []interface{}{point}); err != nil || got != int64(4326) {
		t.Fatalf("ST_SRID = %#v, %v; want 4326", got, err)
	}
	changed, err := evalSpatialFunction("ST_SRID", []interface{}{point, int64(3857)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_SRID", []interface{}{changed}); err != nil || got != int64(3857) {
		t.Fatalf("ST_SRID setter = %#v, %v; want 3857", got, err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{point}); err != nil || got != "POINT(1.25 -2.5)" {
		t.Fatalf("ST_AsText = %#v, %v", got, err)
	}
	if got, err := evalSpatialFunction("ST_GeometryType", []interface{}{point}); err != nil || got != "ST_Point" {
		t.Fatalf("ST_GeometryType = %#v, %v", got, err)
	}
}

func TestSpatialPointAccessAndDistance(t *testing.T) {
	left, err := evalSpatialFunction("ST_MakePoint", []interface{}{int64(0), int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	right, err := evalSpatialFunction("ST_MakePoint", []interface{}{int64(3), int64(4)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_X", []interface{}{right}); err != nil || got != float64(3) {
		t.Fatalf("ST_X = %#v, %v", got, err)
	}
	if got, err := evalSpatialFunction("ST_Y", []interface{}{right}); err != nil || got != float64(4) {
		t.Fatalf("ST_Y = %#v, %v", got, err)
	}
	if got, err := evalSpatialFunction("ST_Distance", []interface{}{left, right}); err != nil || got != float64(5) {
		t.Fatalf("ST_Distance = %#v, %v", got, err)
	}
	equatorStart, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(0 0)"})
	if err != nil {
		t.Fatal(err)
	}
	equatorEnd, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(1 0)"})
	if err != nil {
		t.Fatal(err)
	}
	sphere, err := evalSpatialFunction("ST_Distance_Sphere", []interface{}{equatorStart, equatorEnd})
	if err != nil {
		t.Fatal(err)
	}
	if distance, ok := sphere.(float64); !ok || math.Abs(distance-111194.68229846345) > 0.01 {
		t.Fatalf("ST_Distance_Sphere = %#v, want approximately 111194.68 meters", sphere)
	}
	outOfRange, err := evalSpatialFunction("ST_Distance_Sphere", []interface{}{
		[]byte("POINT(181 0)"), equatorStart,
	})
	if err == nil || outOfRange != float64(0) {
		t.Fatalf("out-of-range ST_Distance_Sphere = %#v, %v; want error", outOfRange, err)
	}
	line, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 0,2 0)"})
	if err != nil {
		t.Fatal(err)
	}
	lineDistance, err := evalSpatialFunction("ST_Distance_Sphere", []interface{}{line, equatorStart})
	if err != nil {
		t.Fatalf("line ST_Distance_Sphere failed: %v", err)
	}
	if distance, ok := lineDistance.(float64); !ok || distance > 0.01 {
		t.Fatalf("line ST_Distance_Sphere = %#v, want zero at a shared vertex", lineDistance)
	}
	lineWithInteriorClosestPoint, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 0,10 0)"})
	if err != nil {
		t.Fatal(err)
	}
	nearLine, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(5 1)"})
	if err != nil {
		t.Fatal(err)
	}
	interiorDistance, err := evalSpatialFunction("ST_Distance_Sphere", []interface{}{lineWithInteriorClosestPoint, nearLine})
	if err != nil {
		t.Fatalf("line interior ST_Distance_Sphere failed: %v", err)
	}
	if distance, ok := interiorDistance.(float64); !ok || math.Abs(distance-111194.68229846345) > 0.1 {
		t.Fatalf("line interior ST_Distance_Sphere = %#v, want approximately 111194.68 meters", interiorDistance)
	}
	customRadius, err := evalSpatialFunction("ST_Distance_Sphere", []interface{}{equatorStart, equatorEnd, float64(6371000)})
	if err != nil {
		t.Fatal(err)
	}
	if distance, ok := customRadius.(float64); !ok || math.Abs(distance-111194.92664455874) > 0.01 {
		t.Fatalf("custom-radius ST_Distance_Sphere = %#v, want approximately 111194.93 meters", customRadius)
	}
	if _, err := evalSpatialFunction("ST_Distance_Sphere", []interface{}{equatorStart, equatorEnd, float64(0)}); err == nil {
		t.Fatal("zero-radius ST_Distance_Sphere should fail")
	}
	crossingLine, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(-10 0,10 0)"})
	if err != nil {
		t.Fatal(err)
	}
	otherCrossingLine, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 -10,0 10)"})
	if err != nil {
		t.Fatal(err)
	}
	crossingDistance, err := evalSpatialFunction("ST_Distance_Sphere", []interface{}{crossingLine, otherCrossingLine})
	if err != nil || crossingDistance != float64(0) {
		t.Fatalf("crossing-line ST_Distance_Sphere = %#v, %v; want zero", crossingDistance, err)
	}
}

func TestSpatialWKBAndPolygonPredicates(t *testing.T) {
	polygon, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POLYGON((0 0,10 0,10 10,0 10,0 0))"})
	if err != nil {
		t.Fatal(err)
	}
	inside, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(5 5)"})
	if err != nil {
		t.Fatal(err)
	}
	outside, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(20 20)"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_Contains", []interface{}{polygon, inside}); err != nil || got != int64(1) {
		t.Fatalf("ST_Contains(inside) = %#v, %v", got, err)
	}
	if got, err := evalSpatialFunction("ST_Within", []interface{}{outside, polygon}); err != nil || got != int64(0) {
		t.Fatalf("ST_Within(outside) = %#v, %v", got, err)
	}
	wkb, err := evalSpatialFunction("ST_AsWKB", []interface{}{inside})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wkb.([]byte), []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x14, 0x40, 0, 0, 0, 0, 0, 0, 0x14, 0x40}) {
		t.Fatalf("unexpected point WKB: %x", wkb)
	}
}

func TestSpatialConvexHullSupportsPointLineAndPolygonInputs(t *testing.T) {
	point, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(2 3)"})
	if err != nil {
		t.Fatal(err)
	}
	hull, err := evalSpatialFunction("ST_ConvexHull", []interface{}{point})
	if err != nil {
		t.Fatalf("point convex hull failed: %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{hull}); err != nil || got != "POINT(2 3)" {
		t.Fatalf("point hull = %#v, %v", got, err)
	}

	line, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 0,2 0,1 0)"})
	if err != nil {
		t.Fatal(err)
	}
	hull, err = evalSpatialFunction("ST_ConvexHull", []interface{}{line})
	if err != nil {
		t.Fatalf("line convex hull failed: %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{hull}); err != nil || got != "LINESTRING(0 0,2 0)" {
		t.Fatalf("line hull = %#v, %v", got, err)
	}

	polygon, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POLYGON((0 0,4 0,4 2,2 4,0 2,0 0))"})
	if err != nil {
		t.Fatal(err)
	}
	hull, err = evalSpatialFunction("ST_ConvexHull", []interface{}{polygon})
	if err != nil {
		t.Fatalf("polygon convex hull failed: %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{hull}); err != nil || got != "POLYGON((0 0,4 0,4 2,2 4,0 2,0 0))" {
		t.Fatalf("polygon hull = %#v, %v", got, err)
	}
}

func TestSpatialTextBytesIntersectWithBinaryGeometry(t *testing.T) {
	point, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(1 1)"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := EvaluateSpatialFunction("ST_INTERSECTS", []interface{}{[]byte("POINT(1 1)"), point})
	if err != nil || got != int64(1) {
		t.Fatalf("ST_INTERSECTS text/binary = %#v, %v", got, err)
	}
}

func TestSpatialGeoJSONRoundTripAndSRID(t *testing.T) {
	geometry, err := evalSpatialFunction("ST_GeomFromGeoJSON", []interface{}{`{"type":"Point","coordinates":[1.25,-2.5]}`, int64(0), int64(4326)})
	if err != nil {
		t.Fatalf("ST_GeomFromGeoJSON failed: %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{geometry}); err != nil || got != "POINT(1.25 -2.5)" {
		t.Fatalf("GeoJSON point WKT = %#v, %v", got, err)
	}
	if got, err := evalSpatialFunction("ST_SRID", []interface{}{geometry}); err != nil || got != int64(4326) {
		t.Fatalf("GeoJSON point SRID = %#v, %v", got, err)
	}
	encoded, err := evalSpatialFunction("ST_AsGeoJSON", []interface{}{geometry})
	if err != nil {
		t.Fatalf("ST_AsGeoJSON failed: %v", err)
	}
	var document map[string]interface{}
	if err := json.Unmarshal([]byte(encoded.(string)), &document); err != nil {
		t.Fatalf("decode GeoJSON output: %v", err)
	}
	if document["type"] != "Point" {
		t.Fatalf("GeoJSON type = %#v", document["type"])
	}
	coordinates, ok := document["coordinates"].([]interface{})
	if !ok || len(coordinates) != 2 || coordinates[0] != 1.25 || coordinates[1] != -2.5 {
		t.Fatalf("GeoJSON coordinates = %#v", document["coordinates"])
	}

	polygon, err := evalSpatialFunction("ST_GeomFromGeoJSON", []interface{}{`{"type":"Polygon","coordinates":[[[0,0],[4,0],[4,4],[0,4],[0,0]]]}`})
	if err != nil {
		t.Fatalf("ST_GeomFromGeoJSON polygon failed: %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{polygon}); err != nil || got != "POLYGON((0 0,4 0,4 4,0 4,0 0))" {
		t.Fatalf("GeoJSON polygon WKT = %#v, %v", got, err)
	}
}

func TestCompileExpressionSupportsSpatialFunctions(t *testing.T) {
	compiled, ok := CompileExpression(&Function{FuncName: "ST_X", FuncArgs: []Expression{
		&Function{FuncName: "ST_MakePoint", FuncArgs: []Expression{&Constant{Value: int64(7)}, &Constant{Value: int64(8)}}},
	}})
	if !ok {
		t.Fatal("spatial function should use the compiled evaluator")
	}
	value, err := compiled(&EvalContext{})
	if err != nil || value != float64(7) {
		t.Fatalf("compiled ST_X = %#v, %v", value, err)
	}
	geoJSON, ok := CompileExpression(&Function{FuncName: "ST_AsGeoJSON", FuncArgs: []Expression{
		&Function{FuncName: "ST_MakePoint", FuncArgs: []Expression{&Constant{Value: int64(1)}, &Constant{Value: int64(2)}}},
	}})
	if !ok {
		t.Fatal("ST_AsGeoJSON should use the compiled evaluator")
	}
	geoJSONValue, err := geoJSON(&EvalContext{})
	if err != nil || geoJSONValue != `{"coordinates":[1,2],"type":"Point"}` {
		t.Fatalf("compiled ST_AsGeoJSON = %#v, %v", geoJSONValue, err)
	}
}

func TestSpatialCollectionWKTAndWKBRoundTrip(t *testing.T) {
	for _, wkt := range []string{
		"MULTIPOINT((1 2),(3 4))",
		"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
		"MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))",
		"GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))",
	} {
		geometry, err := evalSpatialFunction("ST_GeomFromText", []interface{}{wkt})
		if err != nil {
			t.Fatalf("parse %s: %v", wkt, err)
		}
		wkb, err := evalSpatialFunction("ST_AsWKB", []interface{}{geometry})
		if err != nil {
			t.Fatalf("encode %s: %v", wkt, err)
		}
		decoded, err := evalSpatialFunction("ST_GeomFromWKB", []interface{}{wkb})
		if err != nil {
			t.Fatalf("decode %s: %v", wkt, err)
		}
		got, err := evalSpatialFunction("ST_AsText", []interface{}{decoded})
		if err != nil {
			t.Fatalf("render %s: %v", wkt, err)
		}
		if got == "" {
			t.Fatalf("empty round-trip WKT for %s", wkt)
		}
	}
}

func TestSpatialMeasurementsAndCoordinateTransforms(t *testing.T) {
	polygon, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POLYGON((0 0,4 0,4 3,0 3,0 0))", int64(4326)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_Area", []interface{}{polygon}); err != nil || got != float64(12) {
		t.Fatalf("ST_Area = %#v, %v; want 12", got, err)
	}
	if got, err := evalSpatialFunction("ST_Centroid", []interface{}{polygon}); err != nil {
		t.Fatalf("ST_Centroid failed: %v", err)
	} else if text, textErr := evalSpatialFunction("ST_AsText", []interface{}{got}); textErr != nil || text != "POINT(2 1.5)" {
		t.Fatalf("ST_Centroid text = %#v, %v", text, textErr)
	}
	line, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 0,3 4)"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_Length", []interface{}{line}); err != nil || got != float64(5) {
		t.Fatalf("ST_Length = %#v, %v; want 5", got, err)
	}
	point, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(1 2)"})
	if err != nil {
		t.Fatal(err)
	}
	swapped, err := evalSpatialFunction("ST_SwapXY", []interface{}{point})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{swapped}); err != nil || got != "POINT(2 1)" {
		t.Fatalf("ST_SwapXY = %#v, %v", got, err)
	}
	if got, err := evalSpatialFunction("ST_Envelope", []interface{}{line}); err != nil {
		t.Fatal(err)
	} else if text, textErr := evalSpatialFunction("ST_AsText", []interface{}{got}); textErr != nil || text != "POLYGON((0 0,3 0,3 4,0 4,0 0))" {
		t.Fatalf("ST_Envelope = %#v, %v", text, textErr)
	}
}

func TestSpatialMBRPredicates(t *testing.T) {
	box, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POLYGON((0 0,10 0,10 10,0 10,0 0))"})
	if err != nil {
		t.Fatal(err)
	}
	point, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POINT(5 5)"})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int64{"MBRContains": 1, "MBRWithin": 0, "MBRIntersects": 1, "MBREquals": 0, "MBRDisjoint": 0} {
		got, evalErr := evalSpatialFunction(name, []interface{}{box, point})
		if evalErr != nil || got != want {
			t.Fatalf("%s = %#v, %v; want %d", name, got, evalErr, want)
		}
	}
}

func TestSpatialGeometryAccessors(t *testing.T) {
	line, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 0,3 4,3 4)"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_NumPoints", []interface{}{line}); err != nil || got != int64(3) {
		t.Fatalf("ST_NumPoints = %#v, %v", got, err)
	}
	for name, want := range map[string]string{"ST_StartPoint": "POINT(0 0)", "ST_EndPoint": "POINT(3 4)", "ST_PointN": "POINT(3 4)"} {
		args := []interface{}{line}
		if name == "ST_PointN" {
			args = append(args, int64(2))
		}
		value, evalErr := evalSpatialFunction(name, args)
		if evalErr != nil {
			t.Fatalf("%s failed: %v", name, evalErr)
		}
		text, textErr := evalSpatialFunction("ST_AsText", []interface{}{value})
		if textErr != nil || text != want {
			t.Fatalf("%s = %#v, %v", name, text, textErr)
		}
	}
	polygon, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_Dimension", []interface{}{polygon}); err != nil || got != int64(2) {
		t.Fatalf("ST_Dimension = %#v, %v", got, err)
	}
	if got, err := evalSpatialFunction("ST_NumInteriorRing", []interface{}{polygon}); err != nil || got != int64(1) {
		t.Fatalf("ST_NumInteriorRing = %#v, %v", got, err)
	}
	ringValue, err := evalSpatialFunction("ST_ExteriorRing", []interface{}{polygon})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_IsClosed", []interface{}{ringValue}); err != nil || got != int64(1) {
		t.Fatalf("ST_IsClosed = %#v, %v", got, err)
	}
	collection, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"GEOMETRYCOLLECTION(POINT(1 2),POINT(3 4))"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evalSpatialFunction("ST_NumGeometries", []interface{}{collection}); err != nil || got != int64(2) {
		t.Fatalf("ST_NumGeometries = %#v, %v", got, err)
	}
}

func TestSpatialSimplicityAndPointOnSurface(t *testing.T) {
	simple, err := evalSpatialFunction("ST_IS_SIMPLE", []interface{}{"LINESTRING(0 0,1 1,2 0)"})
	if err != nil || simple != int64(1) {
		t.Fatalf("ST_IsSimple(simple line) = %#v, %v; want 1", simple, err)
	}
	complex, err := evalSpatialFunction("ST_IS_SIMPLE", []interface{}{"LINESTRING(0 0,2 2,0 2,2 0)"})
	if err != nil || complex != int64(0) {
		t.Fatalf("ST_IsSimple(self-intersecting line) = %#v, %v; want 0", complex, err)
	}

	point, err := evalSpatialFunction("ST_POINTONSURFACE", []interface{}{"POLYGON((0 0,10 0,10 10,0 10,0 0))"})
	if err != nil {
		t.Fatalf("ST_PointOnSurface() error = %v", err)
	}
	text, err := evalSpatialFunction("ST_ASTEXT", []interface{}{point})
	if err != nil || text != "POINT(5 5)" {
		t.Fatalf("ST_PointOnSurface() = %#v, %v; want POINT(5 5)", text, err)
	}
}

func TestSpatialValidateReturnsOnlyValidGeometry(t *testing.T) {
	valid, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 0,1 1)"})
	if err != nil {
		t.Fatal(err)
	}
	validated, err := evalSpatialFunction("ST_Validate", []interface{}{valid})
	if err != nil {
		t.Fatalf("ST_Validate(valid) error = %v", err)
	}
	text, err := evalSpatialFunction("ST_AsText", []interface{}{validated})
	if err != nil || text != "LINESTRING(0 0,1 1)" {
		t.Fatalf("ST_Validate(valid) = %#v, %v; want original geometry", text, err)
	}

	invalid, err := evalSpatialFunction("ST_GeomFromText", []interface{}{"LINESTRING(0 0)"})
	if err != nil {
		t.Fatal(err)
	}
	validated, err = evalSpatialFunction("ST_Validate", []interface{}{invalid})
	if err != nil {
		t.Fatalf("ST_Validate(invalid) error = %v", err)
	}
	if validated != nil {
		t.Fatalf("ST_Validate(invalid) = %#v; want NULL", validated)
	}

	bowtie := spatialTestGeometry(t, "POLYGON((0 0,2 2,0 2,2 0,0 0))")
	validFlag, err := evalSpatialFunction("ST_IsValid", []interface{}{bowtie})
	if err != nil || validFlag != int64(0) {
		t.Fatalf("ST_IsValid(bowtie) = %#v, %v; want 0", validFlag, err)
	}
	validated, err = evalSpatialFunction("ST_Validate", []interface{}{bowtie})
	if err != nil || validated != nil {
		t.Fatalf("ST_Validate(bowtie) = %#v, %v; want NULL", validated, err)
	}
}

func TestSpatialPointAtDistanceInterpolatesLineString(t *testing.T) {
	line := spatialTestGeometry(t, "LINESTRING(0 0,3 4,6 4)")
	point, err := evalSpatialFunction("ST_PointAtDistance", []interface{}{line, float64(2.5)})
	if err != nil {
		t.Fatalf("ST_PointAtDistance() error = %v", err)
	}
	text, err := evalSpatialFunction("ST_AsText", []interface{}{point})
	if err != nil || text != "POINT(1.5 2)" {
		t.Fatalf("ST_PointAtDistance() = %#v, %v; want POINT(1.5 2)", text, err)
	}

	endpoint, err := evalSpatialFunction("ST_PointAtDistance", []interface{}{line, float64(8)})
	if err != nil {
		t.Fatalf("ST_PointAtDistance(endpoint) error = %v", err)
	}
	text, err = evalSpatialFunction("ST_AsText", []interface{}{endpoint})
	if err != nil || text != "POINT(6 4)" {
		t.Fatalf("ST_PointAtDistance(endpoint) = %#v, %v; want POINT(6 4)", text, err)
	}

	if _, err := evalSpatialFunction("ST_PointAtDistance", []interface{}{line, float64(8.1)}); err == nil {
		t.Fatal("ST_PointAtDistance(out of range) unexpectedly succeeded")
	}
	if _, err := evalSpatialFunction("ST_PointAtDistance", []interface{}{spatialTestGeometry(t, "POINT(0 0)"), float64(0)}); err == nil {
		t.Fatal("ST_PointAtDistance(point) unexpectedly succeeded")
	}

	straightLine := spatialTestGeometry(t, "LINESTRING(0 0,3 4)")
	interpolated, err := evalSpatialFunction("ST_LineInterpolatePoint", []interface{}{straightLine, float64(0.5)})
	if err != nil {
		t.Fatalf("ST_LineInterpolatePoint() error = %v", err)
	}
	text, err = evalSpatialFunction("ST_AsText", []interface{}{interpolated})
	if err != nil || text != "POINT(1.5 2)" {
		t.Fatalf("ST_LineInterpolatePoint() = %#v, %v; want POINT(1.5 2)", text, err)
	}

	points, err := evalSpatialFunction("ST_LineInterpolatePoints", []interface{}{line, float64(0.25)})
	if err != nil {
		t.Fatalf("ST_LineInterpolatePoints() error = %v", err)
	}
	text, err = evalSpatialFunction("ST_AsText", []interface{}{points})
	if err != nil || text != "MULTIPOINT((0 0),(1.2000000000000002 1.6),(2.4000000000000004 3.2),(4 4),(6 4))" {
		t.Fatalf("ST_LineInterpolatePoints() = %#v, %v; want five evenly spaced points", text, err)
	}
}

func TestSpatialGeoHashEncodeAndDecode(t *testing.T) {
	encoded, err := evalSpatialFunction("ST_GeoHash", []interface{}{float64(180), float64(0), int64(10)})
	if err != nil || encoded != "xbpbpbpbpb" {
		t.Fatalf("ST_GeoHash(180,0,10) = %#v, %v; want xbpbpbpbpb", encoded, err)
	}
	decodedLatitude, err := evalSpatialFunction("ST_LatFromGeoHash", []interface{}{encoded})
	if err != nil || decodedLatitude.(float64) < -0.01 || decodedLatitude.(float64) > 0.01 {
		t.Fatalf("ST_LatFromGeoHash(%v) = %#v, %v; want latitude near zero", encoded, decodedLatitude, err)
	}
	decodedLongitude, err := evalSpatialFunction("ST_LongFromGeoHash", []interface{}{encoded})
	if err != nil || decodedLongitude.(float64) < 179.9 || decodedLongitude.(float64) > 180 {
		t.Fatalf("ST_LongFromGeoHash(%v) = %#v, %v; want longitude near 180", encoded, decodedLongitude, err)
	}

	point, err := evalSpatialFunction("ST_PointFromGeoHash", []interface{}{encoded, int64(4326)})
	if err != nil {
		t.Fatalf("ST_PointFromGeoHash() error = %v", err)
	}
	if srid, err := evalSpatialFunction("ST_SRID", []interface{}{point}); err != nil || srid != int64(4326) {
		t.Fatalf("ST_PointFromGeoHash() SRID = %#v, %v; want 4326", srid, err)
	}

	geometry := spatialTestGeometry(t, "POINT(45 -20)")
	encoded, err = evalSpatialFunction("ST_GeoHash", []interface{}{geometry, int64(10)})
	if err != nil || encoded == "" {
		t.Fatalf("ST_GeoHash(point) = %#v, %v; want non-empty hash", encoded, err)
	}
}

func TestSpatialTypedConstructorsAndEnvelope(t *testing.T) {
	line, err := evalSpatialFunction("ST_LineString", []interface{}{
		spatialTestGeometry(t, "POINT(1 2)"),
		spatialTestGeometry(t, "POINT(4 6)"),
	})
	if err != nil {
		t.Fatalf("ST_LineString() error = %v", err)
	}
	text, err := evalSpatialFunction("ST_AsText", []interface{}{line})
	if err != nil || text != "LINESTRING(1 2,4 6)" {
		t.Fatalf("ST_LineString() = %#v, %v; want LINESTRING(1 2,4 6)", text, err)
	}

	envelope, err := evalSpatialFunction("ST_MakeEnvelope", []interface{}{
		spatialTestGeometry(t, "POINT(4 6)"),
		spatialTestGeometry(t, "POINT(1 2)"),
	})
	if err != nil {
		t.Fatalf("ST_MakeEnvelope() error = %v", err)
	}
	text, err = evalSpatialFunction("ST_AsText", []interface{}{envelope})
	if err != nil || text != "POLYGON((1 2,4 2,4 6,1 6,1 2))" {
		t.Fatalf("ST_MakeEnvelope() = %#v, %v; want rectangular polygon", text, err)
	}

	for name, wkt := range map[string]string{
		"ST_LineFromText":     "LINESTRING(0 0,1 1)",
		"ST_PolyFromText":     "POLYGON((0 0,1 0,1 1,0 0))",
		"ST_MPointFromText":   "MULTIPOINT((0 0),(1 1))",
		"ST_MLineFromText":    "MULTILINESTRING((0 0,1 1))",
		"ST_MPolyFromText":    "MULTIPOLYGON(((0 0,1 0,1 1,0 0)))",
		"ST_GeomCollFromText": "GEOMETRYCOLLECTION(POINT(0 0))",
	} {
		value, parseErr := evalSpatialFunction(name, []interface{}{wkt})
		if parseErr != nil {
			t.Fatalf("%s() error = %v", name, parseErr)
		}
		if _, textErr := evalSpatialFunction("ST_AsText", []interface{}{value}); textErr != nil {
			t.Fatalf("ST_AsText(%s()) error = %v", name, textErr)
		}
	}
}

func TestSpatialSimplifyReducesLineVertices(t *testing.T) {
	line := spatialTestGeometry(t, "LINESTRING(0 0,1 0,2 0,3 0)")
	simplified, err := evalSpatialFunction("ST_Simplify", []interface{}{line, float64(0.1)})
	if err != nil {
		t.Fatalf("ST_Simplify() error = %v", err)
	}
	text, err := evalSpatialFunction("ST_AsText", []interface{}{simplified})
	if err != nil || text != "LINESTRING(0 0,3 0)" {
		t.Fatalf("ST_Simplify() = %#v, %v; want LINESTRING(0 0,3 0)", text, err)
	}
	if _, err := evalSpatialFunction("ST_Simplify", []interface{}{line, float64(-1)}); err == nil {
		t.Fatal("ST_Simplify(negative tolerance) unexpectedly succeeded")
	}
}

func TestSpatialTransformSupportsWGS84AndWebMercator(t *testing.T) {
	point := spatialTestGeometry(t, "POINT(180 0)")
	pointWithSRID, err := evalSpatialFunction("ST_SRID", []interface{}{point, int64(4326)})
	if err != nil {
		t.Fatal(err)
	}
	transformed, err := evalSpatialFunction("ST_Transform", []interface{}{pointWithSRID, int64(3857)})
	if err != nil {
		t.Fatalf("ST_Transform(4326,3857) error = %v", err)
	}
	srid, err := evalSpatialFunction("ST_SRID", []interface{}{transformed})
	if err != nil || srid != int64(3857) {
		t.Fatalf("ST_Transform SRID = %#v, %v; want 3857", srid, err)
	}
	x, err := evalSpatialFunction("ST_X", []interface{}{transformed})
	if err != nil || math.Abs(x.(float64)-20037508.342789244) > 0.01 {
		t.Fatalf("ST_Transform X = %#v, %v; want Web Mercator longitude", x, err)
	}
	y, err := evalSpatialFunction("ST_Y", []interface{}{transformed})
	if err != nil || math.Abs(y.(float64)) > 0.01 {
		t.Fatalf("ST_Transform Y = %#v, %v; want zero", y, err)
	}

	identity, err := evalSpatialFunction("ST_Transform", []interface{}{transformed, int64(3857)})
	if err != nil {
		t.Fatalf("ST_Transform(identity) error = %v", err)
	}
	if srid, err := evalSpatialFunction("ST_SRID", []interface{}{identity}); err != nil || srid != int64(3857) {
		t.Fatalf("ST_Transform(identity) SRID = %#v, %v; want 3857", srid, err)
	}
}

func TestSpatialDiscreteDistanceFunctions(t *testing.T) {
	left := spatialTestGeometry(t, "LINESTRING(0 0,3 4)")
	right := spatialTestGeometry(t, "LINESTRING(0 0,6 8)")
	hausdorff, err := evalSpatialFunction("ST_HausdorffDistance", []interface{}{left, right})
	if err != nil || math.Abs(hausdorff.(float64)-5) > 1e-9 {
		t.Fatalf("ST_HausdorffDistance() = %#v, %v; want 5", hausdorff, err)
	}
	frechet, err := evalSpatialFunction("ST_FrechetDistance", []interface{}{left, right})
	if err != nil || math.Abs(frechet.(float64)-5) > 1e-9 {
		t.Fatalf("ST_FrechetDistance() = %#v, %v; want 5", frechet, err)
	}
}

func TestSpatialBufferBuildsCartesianPointDisk(t *testing.T) {
	point := spatialTestGeometry(t, "POINT(10 20)")
	buffered, err := evalSpatialFunction("ST_Buffer", []interface{}{point, float64(2)})
	if err != nil {
		t.Fatalf("ST_Buffer(point, distance) error = %v", err)
	}
	geometry, err := spatialDecodeArgument(buffered)
	if err != nil {
		t.Fatalf("decode ST_Buffer result: %v", err)
	}
	if geometry.typeID != 3 || len(geometry.rings) != 1 || len(geometry.rings[0]) < 16 {
		t.Fatalf("ST_Buffer(point, 2) = %#v; want a polygon with a stable circular approximation", geometry)
	}
	if math.Abs(geometry.rings[0][0].x-12) > 1e-9 || math.Abs(geometry.rings[0][0].y-20) > 1e-9 {
		t.Fatalf("ST_Buffer first vertex = %#v; want (12,20)", geometry.rings[0][0])
	}
	if math.Abs(spatialArea(geometry)-math.Pi*4) > 0.12 {
		t.Fatalf("ST_Buffer area = %v; want approximately %v", spatialArea(geometry), math.Pi*4)
	}

	unchanged, err := evalSpatialFunction("ST_Buffer", []interface{}{point, float64(0)})
	if err != nil {
		t.Fatalf("ST_Buffer(point, 0) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{unchanged}); err != nil || got != "POINT(10 20)" {
		t.Fatalf("ST_Buffer(point, 0) = %#v, %v; want original point", got, err)
	}
	if _, err := evalSpatialFunction("ST_Buffer", []interface{}{point, float64(-1)}); err == nil {
		t.Fatal("ST_Buffer(point, negative distance) unexpectedly succeeded")
	}
}

func TestSpatialBufferBuildsTwoPointLineCapsule(t *testing.T) {
	line := spatialTestGeometry(t, "LINESTRING(0 0,10 0)")
	buffered, err := evalSpatialFunction("ST_Buffer", []interface{}{line, float64(1)})
	if err != nil {
		t.Fatalf("ST_Buffer(line, distance) error = %v", err)
	}
	geometry, err := spatialDecodeArgument(buffered)
	if err != nil {
		t.Fatalf("decode ST_Buffer result: %v", err)
	}
	if geometry.typeID != 3 || len(geometry.rings) != 1 || len(geometry.rings[0]) < 16 {
		t.Fatalf("ST_Buffer(line, 1) = %#v; want a capsule polygon", geometry)
	}
	if math.Abs(spatialArea(geometry)-(20+math.Pi)) > 0.2 {
		t.Fatalf("ST_Buffer(line, 1) area = %v; want approximately %v", spatialArea(geometry), 20+math.Pi)
	}

	threePointLine := spatialTestGeometry(t, "LINESTRING(0 0,5 0,10 5)")
	if _, err := evalSpatialFunction("ST_Buffer", []interface{}{threePointLine, float64(1)}); err == nil {
		t.Fatal("ST_Buffer(multi-segment line) unexpectedly succeeded")
	}
}

func TestSpatialIntersectionSupportsPointsAndAxisAlignedRectangles(t *testing.T) {
	point := spatialTestGeometry(t, "POINT(2 2)")
	inside, err := evalSpatialFunction("ST_Intersection", []interface{}{point, spatialTestGeometry(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")})
	if err != nil {
		t.Fatalf("ST_Intersection(point, polygon) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{inside}); err != nil || got != "POINT(2 2)" {
		t.Fatalf("ST_Intersection(point, polygon) = %#v, %v; want point", got, err)
	}

	overlap, err := evalSpatialFunction("ST_Intersection", []interface{}{
		spatialTestGeometry(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))"),
		spatialTestGeometry(t, "POLYGON((2 1,6 1,6 3,2 3,2 1))"),
	})
	if err != nil {
		t.Fatalf("ST_Intersection(polygon, polygon) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{overlap}); err != nil || got != "POLYGON((2 1,4 1,4 3,2 3,2 1))" {
		t.Fatalf("ST_Intersection(polygon, polygon) = %#v, %v; want overlap rectangle", got, err)
	}

	disjoint, err := evalSpatialFunction("ST_Intersection", []interface{}{point, spatialTestGeometry(t, "POINT(3 3)")})
	if err != nil {
		t.Fatalf("ST_Intersection(disjoint points) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{disjoint}); err != nil || got != "GEOMETRYCOLLECTION EMPTY" {
		t.Fatalf("ST_Intersection(disjoint points) = %#v, %v; want empty collection", got, err)
	}
}

func TestSpatialUnionSupportsPointsAndDisjointRectangles(t *testing.T) {
	leftPoint := spatialTestGeometry(t, "POINT(1 1)")
	rightPoint := spatialTestGeometry(t, "POINT(2 2)")
	union, err := evalSpatialFunction("ST_Union", []interface{}{leftPoint, rightPoint})
	if err != nil {
		t.Fatalf("ST_Union(point, point) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{union}); err != nil || got != "MULTIPOINT((1 1),(2 2))" {
		t.Fatalf("ST_Union(point, point) = %#v, %v; want multipoint", got, err)
	}

	outer := spatialTestGeometry(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")
	inner := spatialTestGeometry(t, "POLYGON((1 1,2 1,2 2,1 2,1 1))")
	union, err = evalSpatialFunction("ST_Union", []interface{}{outer, inner})
	if err != nil {
		t.Fatalf("ST_Union(containing polygons) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{union}); err != nil || got != "POLYGON((0 0,4 0,4 4,0 4,0 0))" {
		t.Fatalf("ST_Union(containing polygons) = %#v, %v; want outer polygon", got, err)
	}

	disjoint, err := evalSpatialFunction("ST_Union", []interface{}{
		spatialTestGeometry(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))"),
		spatialTestGeometry(t, "POLYGON((3 3,4 3,4 4,3 4,3 3))"),
	})
	if err != nil {
		t.Fatalf("ST_Union(disjoint polygons) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{disjoint}); err != nil || got != "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((3 3,4 3,4 4,3 4,3 3)))" {
		t.Fatalf("ST_Union(disjoint polygons) = %#v, %v; want multipolygon", got, err)
	}
}

func TestSpatialDifferenceSupportsPointAndContainingRectangleCases(t *testing.T) {
	point := spatialTestGeometry(t, "POINT(1 1)")
	polygon := spatialTestGeometry(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")
	remaining, err := evalSpatialFunction("ST_Difference", []interface{}{point, polygon})
	if err != nil {
		t.Fatalf("ST_Difference(point, polygon) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{remaining}); err != nil || got != "POINT EMPTY" {
		t.Fatalf("ST_Difference(point, polygon) = %#v, %v; want empty point", got, err)
	}

	outside := spatialTestGeometry(t, "POINT(5 5)")
	remaining, err = evalSpatialFunction("ST_Difference", []interface{}{outside, polygon})
	if err != nil {
		t.Fatalf("ST_Difference(outside point, polygon) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{remaining}); err != nil || got != "POINT(5 5)" {
		t.Fatalf("ST_Difference(outside point, polygon) = %#v, %v; want original point", got, err)
	}

	same, err := evalSpatialFunction("ST_Difference", []interface{}{polygon, polygon})
	if err != nil {
		t.Fatalf("ST_Difference(same polygon) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{same}); err != nil || got != "GEOMETRYCOLLECTION EMPTY" {
		t.Fatalf("ST_Difference(same polygon) = %#v, %v; want empty collection", got, err)
	}
}

func TestSpatialSymDifferenceSupportsDistinctPoints(t *testing.T) {
	left := spatialTestGeometry(t, "POINT(1 1)")
	right := spatialTestGeometry(t, "POINT(2 2)")
	value, err := evalSpatialFunction("ST_SymDifference", []interface{}{left, right})
	if err != nil {
		t.Fatalf("ST_SymDifference(point, point) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{value}); err != nil || got != "MULTIPOINT((1 1),(2 2))" {
		t.Fatalf("ST_SymDifference(point, point) = %#v, %v; want multipoint", got, err)
	}

	equal, err := evalSpatialFunction("ST_SymDifference", []interface{}{left, left})
	if err != nil {
		t.Fatalf("ST_SymDifference(equal points) error = %v", err)
	}
	if got, err := evalSpatialFunction("ST_AsText", []interface{}{equal}); err != nil || got != "GEOMETRYCOLLECTION EMPTY" {
		t.Fatalf("ST_SymDifference(equal points) = %#v, %v; want empty collection", got, err)
	}
}

func TestSpatialPredicatesSupportLinesAndPolygons(t *testing.T) {
	lineA := spatialTestGeometry(t, "LINESTRING(0 0,10 10)")
	lineB := spatialTestGeometry(t, "LINESTRING(0 10,10 0)")
	if got, err := evalSpatialFunction("ST_INTERSECTS", []interface{}{lineA, lineB}); err != nil || got != int64(1) {
		t.Fatalf("ST_Intersects(line,line) = %#v, %v; want 1", got, err)
	}

	polygon := spatialTestGeometry(t, "POLYGON((0 0,10 0,10 10,0 10,0 0))")
	line := spatialTestGeometry(t, "LINESTRING(2 2,8 8)")
	if got, err := evalSpatialFunction("ST_CONTAINS", []interface{}{polygon, line}); err != nil || got != int64(1) {
		t.Fatalf("ST_Contains(polygon,line) = %#v, %v; want 1", got, err)
	}
	if got, err := evalSpatialFunction("ST_WITHIN", []interface{}{line, polygon}); err != nil || got != int64(1) {
		t.Fatalf("ST_Within(line,polygon) = %#v, %v; want 1", got, err)
	}
	innerPolygon := spatialTestGeometry(t, "POLYGON((1 1,4 1,4 4,1 4,1 1))")
	if got, err := evalSpatialFunction("ST_CONTAINS", []interface{}{polygon, innerPolygon}); err != nil || got != int64(1) {
		t.Fatalf("ST_Contains(polygon,polygon) = %#v, %v; want 1", got, err)
	}

	overlap := spatialTestGeometry(t, "POLYGON((5 5,15 5,15 15,5 15,5 5))")
	if got, err := evalSpatialFunction("ST_INTERSECTS", []interface{}{polygon, overlap}); err != nil || got != int64(1) {
		t.Fatalf("ST_Intersects(polygon,polygon) = %#v, %v; want 1", got, err)
	}
}

func TestSpatialDistanceSupportsLineGeometry(t *testing.T) {
	lineA := spatialTestGeometry(t, "LINESTRING(0 0,10 0)")
	lineB := spatialTestGeometry(t, "LINESTRING(0 3,10 3)")
	got, err := evalSpatialFunction("ST_DISTANCE", []interface{}{lineA, lineB})
	if err != nil || got != float64(3) {
		t.Fatalf("ST_Distance(line,line) = %#v, %v; want 3", got, err)
	}
	point := spatialTestGeometry(t, "POINT(4 5)")
	got, err = evalSpatialFunction("ST_DISTANCE", []interface{}{lineA, point})
	if err != nil || got != float64(5) {
		t.Fatalf("ST_Distance(line,point) = %#v, %v; want 5", got, err)
	}
}

func TestSpatialDisjointPredicate(t *testing.T) {
	left := spatialTestGeometry(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")
	right := spatialTestGeometry(t, "POLYGON((10 10,14 10,14 14,10 14,10 10))")
	if got, err := evalSpatialFunction("ST_DISJOINT", []interface{}{left, right}); err != nil || got != int64(1) {
		t.Fatalf("ST_Disjoint(disjoint polygons) = %#v, %v; want 1", got, err)
	}
	if got, err := evalSpatialFunction("ST_DISJOINT", []interface{}{left, spatialTestGeometry(t, "POINT(2 2)")}); err != nil || got != int64(0) {
		t.Fatalf("ST_Disjoint(contained point) = %#v, %v; want 0", got, err)
	}
}

func TestSpatialTouchesAndOverlapsPredicates(t *testing.T) {
	left := spatialTestGeometry(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")
	touching := spatialTestGeometry(t, "POLYGON((4 0,8 0,8 4,4 4,4 0))")
	overlapping := spatialTestGeometry(t, "POLYGON((2 2,6 2,6 6,2 6,2 2))")
	if got, err := evalSpatialFunction("ST_TOUCHES", []interface{}{left, touching}); err != nil || got != int64(1) {
		t.Fatalf("ST_Touches(shared edge) = %#v, %v; want 1", got, err)
	}
	if got, err := evalSpatialFunction("ST_OVERLAPS", []interface{}{left, overlapping}); err != nil || got != int64(1) {
		t.Fatalf("ST_Overlaps(partial polygons) = %#v, %v; want 1", got, err)
	}
}

func TestSpatialCrossesPredicate(t *testing.T) {
	lineA := spatialTestGeometry(t, "LINESTRING(0 0,10 10)")
	lineB := spatialTestGeometry(t, "LINESTRING(0 10,10 0)")
	if got, err := evalSpatialFunction("ST_CROSSES", []interface{}{lineA, lineB}); err != nil || got != int64(1) {
		t.Fatalf("ST_Crosses(proper line intersection) = %#v, %v; want 1", got, err)
	}
	endpointTouch := spatialTestGeometry(t, "LINESTRING(10 10,20 20)")
	if got, err := evalSpatialFunction("ST_CROSSES", []interface{}{lineA, endpointTouch}); err != nil || got != int64(0) {
		t.Fatalf("ST_Crosses(endpoint touch) = %#v, %v; want 0", got, err)
	}
	polygon := spatialTestGeometry(t, "POLYGON((0 0,10 0,10 10,0 10,0 0))")
	through := spatialTestGeometry(t, "LINESTRING(-1 5,11 5)")
	inside := spatialTestGeometry(t, "LINESTRING(2 2,8 8)")
	if got, err := evalSpatialFunction("ST_CROSSES", []interface{}{through, polygon}); err != nil || got != int64(1) {
		t.Fatalf("ST_Crosses(line through polygon) = %#v, %v; want 1", got, err)
	}
	if got, err := evalSpatialFunction("ST_CROSSES", []interface{}{inside, polygon}); err != nil || got != int64(0) {
		t.Fatalf("ST_Crosses(line contained by polygon) = %#v, %v; want 0", got, err)
	}
}

func spatialTestGeometry(t *testing.T, wkt string) []byte {
	t.Helper()
	geometry, err := evalSpatialFunction("ST_GEOMFROMTEXT", []interface{}{wkt})
	if err != nil {
		t.Fatalf("parse %s: %v", wkt, err)
	}
	value, ok := geometry.([]byte)
	if !ok {
		t.Fatalf("geometry %s has type %T", wkt, geometry)
	}
	return value
}
