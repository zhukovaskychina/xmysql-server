package plan

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

type spatialPoint struct {
	x float64
	y float64
}

type spatialGeometry struct {
	srid     uint32
	typeID   uint32
	points   []spatialPoint
	rings    [][]spatialPoint
	children []spatialGeometry
}

// EvaluateSpatialFunction exposes the bounded spatial compatibility evaluator
// to engine paths that need to apply a spatial predicate to decoded storage
// values. The normal SQL expression path continues to call the private
// evaluator through Function.Eval.
func EvaluateSpatialFunction(name string, args []interface{}) (interface{}, error) {
	return evalSpatialFunction(name, args)
}

func evalSpatialFunction(name string, args []interface{}) (interface{}, error) {
	name = strings.ToUpper(strings.TrimSpace(name))
	switch name {
	case "ST_LINEFROMTEXT", "ST_LINESTRINGFROMTEXT", "ST_POLYFROMTEXT", "ST_POLYGONFROMTEXT",
		"ST_MPOINTFROMTEXT", "ST_MULTIPOINTFROMTEXT", "ST_MLINEFROMTEXT", "ST_MULTILINESTRINGFROMTEXT",
		"ST_MPOLYFROMTEXT", "ST_MULTIPOLYGONFROMTEXT", "ST_GEOMCOLLFROMTEXT", "ST_GEOMETRYCOLLECTIONFROMTEXT", "ST_GEOMCOLLFROMTXT":
		if len(args) < 1 || len(args) > 2 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialParseWKT(compatString(args[0]))
		if err != nil {
			return nil, err
		}
		expectedType := map[string]uint32{
			"ST_LINEFROMTEXT": 2, "ST_LINESTRINGFROMTEXT": 2,
			"ST_POLYFROMTEXT": 3, "ST_POLYGONFROMTEXT": 3,
			"ST_MPOINTFROMTEXT": 4, "ST_MULTIPOINTFROMTEXT": 4,
			"ST_MLINEFROMTEXT": 5, "ST_MULTILINESTRINGFROMTEXT": 5,
			"ST_MPOLYFROMTEXT": 6, "ST_MULTIPOLYGONFROMTEXT": 6,
			"ST_GEOMCOLLFROMTEXT": 7, "ST_GEOMETRYCOLLECTIONFROMTEXT": 7, "ST_GEOMCOLLFROMTXT": 7,
		}[name]
		if geometry.typeID != expectedType {
			return nil, fmt.Errorf("unexpected geometry type")
		}
		if len(args) == 2 && args[1] != nil {
			srid, sridErr := integerArg(args[1])
			if sridErr != nil || srid < 0 || srid > math.MaxUint32 {
				return nil, fmt.Errorf("invalid geometry SRID")
			}
			geometry.srid = uint32(srid)
		}
		return spatialEncodeInternal(geometry), nil

	case "ST_GEOMFROMTEXT", "ST_GEOMETRYFROMTEXT", "GEOMFROMTEXT",
		"ST_POINTFROMTEXT", "POINTFROMTEXT":
		if len(args) < 1 || len(args) > 2 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialParseWKT(compatString(args[0]))
		if err != nil {
			return nil, err
		}
		if len(args) == 2 && args[1] != nil {
			srid, err := integerArg(args[1])
			if err != nil || srid < 0 || srid > math.MaxUint32 {
				return nil, fmt.Errorf("invalid geometry SRID")
			}
			geometry.srid = uint32(srid)
		}
		if strings.Contains(name, "POINTFROMTEXT") && geometry.typeID != 1 {
			return nil, fmt.Errorf("Point object expected")
		}
		return spatialEncodeInternal(geometry), nil

	case "ST_GEOMFROMWKB", "ST_GEOMETRYFROMWKB", "GEOMFROMWKB",
		"ST_POINTFROMWKB", "POINTFROMWKB":
		if len(args) < 1 || len(args) > 2 || args[0] == nil {
			return nil, nil
		}
		data, ok := spatialBytes(args[0])
		if !ok {
			return nil, fmt.Errorf("geometry WKB must be binary")
		}
		geometry, err := spatialDecode(data)
		if err != nil {
			return nil, err
		}
		if len(args) == 2 && args[1] != nil {
			srid, err := integerArg(args[1])
			if err != nil || srid < 0 || srid > math.MaxUint32 {
				return nil, fmt.Errorf("invalid geometry SRID")
			}
			geometry.srid = uint32(srid)
		}
		if strings.Contains(name, "POINTFROMWKB") && geometry.typeID != 1 {
			return nil, fmt.Errorf("Point object expected")
		}
		return spatialEncodeInternal(geometry), nil

	case "ST_GEOMFROMGEOJSON", "ST_GEOMETRYFROMGEOJSON":
		if len(args) < 1 || len(args) > 3 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialParseGeoJSON(compatString(args[0]))
		if err != nil {
			return nil, err
		}
		if len(args) >= 2 && args[1] != nil {
			options, optionsErr := integerArg(args[1])
			if optionsErr != nil || options < 0 || options > 2 {
				return nil, fmt.Errorf("unsupported GeoJSON options")
			}
		}
		if len(args) == 3 && args[2] != nil {
			srid, sridErr := integerArg(args[2])
			if sridErr != nil || srid < 0 || srid > math.MaxUint32 {
				return nil, fmt.Errorf("invalid geometry SRID")
			}
			geometry.srid = uint32(srid)
		}
		return spatialEncodeInternal(geometry), nil

	case "ST_MAKEPOINT", "POINT":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		x, err := numericArg(args[0])
		if err != nil {
			return nil, err
		}
		y, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		return spatialEncodeInternal(spatialGeometry{typeID: 1, points: []spatialPoint{{x: x, y: y}}}), nil

	case "ST_LINESTRING", "LINESTRING":
		if len(args) < 2 {
			return nil, nil
		}
		line := spatialGeometry{typeID: 2}
		for _, argument := range args {
			point, err := spatialPointArgument([]interface{}{argument})
			if err != nil {
				return nil, err
			}
			if point == nil {
				return nil, nil
			}
			if len(line.points) == 0 {
				line.srid = point.srid
			} else if line.srid != point.srid {
				return nil, fmt.Errorf("spatial reference systems do not match")
			}
			line.points = append(line.points, point.points[0])
		}
		return spatialEncodeInternal(line), nil

	case "ST_MAKEENVELOPE":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := spatialPointArgument([]interface{}{args[0]})
		if err != nil {
			return nil, err
		}
		right, err := spatialPointArgument([]interface{}{args[1]})
		if err != nil {
			return nil, err
		}
		if left.srid != right.srid {
			return nil, fmt.Errorf("spatial reference systems do not match")
		}
		minX, maxX := math.Min(left.points[0].x, right.points[0].x), math.Max(left.points[0].x, right.points[0].x)
		minY, maxY := math.Min(left.points[0].y, right.points[0].y), math.Max(left.points[0].y, right.points[0].y)
		return spatialEncodeInternal(spatialGeometry{srid: left.srid, typeID: 3, rings: [][]spatialPoint{{
			{x: minX, y: minY}, {x: maxX, y: minY}, {x: maxX, y: maxY}, {x: minX, y: maxY}, {x: minX, y: minY},
		}}}), nil

	case "ST_ASTEXT", "ASTEXT":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return spatialWKT(geometry), nil

	case "ST_ASGEOJSON":
		if len(args) < 1 || len(args) > 3 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if len(args) >= 2 && args[1] != nil {
			maxDigits, digitsErr := integerArg(args[1])
			if digitsErr != nil || maxDigits < 0 {
				return nil, fmt.Errorf("invalid ST_AsGeoJSON max_dec_digits")
			}
		}
		return spatialGeoJSON(geometry)

	case "ST_ASWKB", "ASWKB":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return spatialEncodeWKB(geometry), nil

	case "ST_SRID", "SRID":
		if (len(args) != 1 && len(args) != 2) || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if len(args) == 2 {
			srid, sridErr := integerArg(args[1])
			if sridErr != nil || srid < 0 || srid > math.MaxUint32 {
				return nil, fmt.Errorf("invalid geometry SRID")
			}
			geometry.srid = uint32(srid)
			return spatialEncodeInternal(geometry), nil
		}
		return int64(geometry.srid), nil

	case "ST_TRANSFORM":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		targetSRID, err := integerArg(args[1])
		if err != nil || targetSRID < 0 || targetSRID > math.MaxUint32 {
			return nil, fmt.Errorf("invalid geometry SRID")
		}
		if geometry.srid == uint32(targetSRID) {
			return spatialEncodeInternal(geometry), nil
		}
		if !((geometry.srid == 4326 && targetSRID == 3857) || (geometry.srid == 3857 && targetSRID == 4326)) {
			return nil, fmt.Errorf("unsupported spatial reference system transformation")
		}
		transformed, ok := spatialTransformGeometry(geometry, uint32(targetSRID))
		if !ok {
			return nil, fmt.Errorf("geometry coordinate is out of range")
		}
		return spatialEncodeInternal(transformed), nil

	case "ST_GEOMETRYTYPE", "GEOMETRYTYPE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return spatialTypeName(geometry.typeID), nil

	case "ST_DIMENSION", "DIMENSION":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return int64(spatialDimension(geometry)), nil

	case "ST_NUMGEOMETRIES", "NUMGEOMETRIES":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if geometry.typeID >= 4 && geometry.typeID <= 7 {
			return int64(len(geometry.children)), nil
		}
		return int64(1), nil

	case "ST_NUMPOINTS", "NUMPOINTS":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return int64(spatialPointCount(geometry)), nil

	case "ST_GEOMETRYN", "GEOMETRYN":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		index, err := integerArg(args[1])
		if err != nil || index < 1 || index > int64(len(geometry.children)) || geometry.typeID < 4 || geometry.typeID > 7 {
			return nil, nil
		}
		child := geometry.children[index-1]
		child.srid = geometry.srid
		return spatialEncodeInternal(child), nil

	case "ST_POINTN", "POINTN", "ST_STARTPOINT", "STARTPOINT", "ST_ENDPOINT", "ENDPOINT":
		if len(args) != 1 && len(args) != 2 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if geometry.typeID != 2 || len(geometry.points) == 0 {
			return nil, nil
		}
		index := int64(1)
		if name == "ST_ENDPOINT" || name == "ENDPOINT" {
			index = int64(len(geometry.points))
		} else if len(args) == 2 {
			index, err = integerArg(args[1])
			if err != nil {
				return nil, err
			}
		}
		if index < 1 || index > int64(len(geometry.points)) {
			return nil, nil
		}
		return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{geometry.points[index-1]}}), nil

	case "ST_NUMINTERIORRING", "NUMINTERIORRING":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if geometry.typeID != 3 {
			return int64(0), nil
		}
		count := len(geometry.rings) - 1
		if count < 0 {
			count = 0
		}
		return int64(count), nil

	case "ST_EXTERIORRING", "EXTERIORRING", "ST_INTERIORRINGN", "INTERIORRINGN":
		if len(args) != 1 && len(args) != 2 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if geometry.typeID != 3 || len(geometry.rings) == 0 {
			return nil, nil
		}
		index := int64(1)
		if name == "ST_EXTERIORRING" || name == "EXTERIORRING" {
			index = 0
		} else {
			index, err = integerArg(args[1])
			if err != nil {
				return nil, err
			}
			if index < 1 {
				return nil, nil
			}
		}
		if index >= int64(len(geometry.rings)) {
			return nil, nil
		}
		return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 2, points: append([]spatialPoint(nil), geometry.rings[index]...)}), nil

	case "ST_ISCLOSED", "ISCLOSED", "ST_ISRING", "ISRING":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return int64(boolToInt(spatialGeometryClosed(geometry))), nil

	case "ST_AREA", "AREA":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return spatialArea(geometry), nil

	case "ST_LENGTH", "LENGTH_GEOMETRY":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return spatialLength(geometry), nil

	case "ST_CENTROID", "CENTROID":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		centroid, ok := spatialCentroid(geometry)
		if !ok {
			return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 1}), nil
		}
		return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{centroid}}), nil

	case "ST_ENVELOPE", "ENVELOPE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return spatialEncodeInternal(spatialEnvelope(geometry)), nil

	case "ST_CONVEXHULL", "CONVEXHULL":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return spatialEncodeInternal(spatialConvexHull(geometry)), nil

	case "ST_BUFFER":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		distance, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		if math.IsNaN(distance) || math.IsInf(distance, 0) {
			return nil, fmt.Errorf("invalid buffer distance")
		}
		if distance == 0 {
			return spatialEncodeInternal(geometry), nil
		}
		if distance < 0 {
			return nil, fmt.Errorf("negative buffer distance is not supported for this geometry")
		}
		switch geometry.typeID {
		case 1:
			if len(geometry.points) == 0 {
				return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 3}), nil
			}
			return spatialEncodeInternal(spatialPointBuffer(geometry, distance)), nil
		case 2:
			if len(geometry.points) != 2 {
				return nil, fmt.Errorf("ST_Buffer currently supports two-point LineStrings only")
			}
			return spatialEncodeInternal(spatialLineBuffer(geometry, distance)), nil
		default:
			return nil, fmt.Errorf("ST_Buffer currently supports Point and two-point LineString geometries only")
		}

	case "ST_INTERSECTION":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		right, err := spatialDecodeArgument(args[1])
		if err != nil {
			return nil, err
		}
		if left.srid != right.srid {
			return nil, fmt.Errorf("spatial reference systems do not match")
		}
		intersection, supported := spatialIntersection(left, right)
		if !supported {
			return nil, fmt.Errorf("ST_Intersection currently supports points and axis-aligned rectangle polygons only")
		}
		return spatialEncodeInternal(intersection), nil

	case "ST_UNION":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		right, err := spatialDecodeArgument(args[1])
		if err != nil {
			return nil, err
		}
		if left.srid != right.srid {
			return nil, fmt.Errorf("spatial reference systems do not match")
		}
		union, supported := spatialUnion(left, right)
		if !supported {
			return nil, fmt.Errorf("ST_Union currently supports points and disjoint or containing rectangle polygons only")
		}
		return spatialEncodeInternal(union), nil

	case "ST_DIFFERENCE":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		right, err := spatialDecodeArgument(args[1])
		if err != nil {
			return nil, err
		}
		if left.srid != right.srid {
			return nil, fmt.Errorf("spatial reference systems do not match")
		}
		difference, supported := spatialDifference(left, right)
		if !supported {
			return nil, fmt.Errorf("ST_Difference currently supports points and point/rectangle cases only")
		}
		return spatialEncodeInternal(difference), nil

	case "ST_SYMDIFFERENCE":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		right, err := spatialDecodeArgument(args[1])
		if err != nil {
			return nil, err
		}
		if left.srid != right.srid {
			return nil, fmt.Errorf("spatial reference systems do not match")
		}
		symmetric, supported := spatialSymmetricDifference(left, right)
		if !supported {
			return nil, fmt.Errorf("ST_SymDifference currently supports point geometries only")
		}
		return spatialEncodeInternal(symmetric), nil

	case "ST_SIMPLIFY":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		tolerance, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		if math.IsNaN(tolerance) || math.IsInf(tolerance, 0) || tolerance < 0 {
			return nil, fmt.Errorf("invalid simplify tolerance")
		}
		return spatialEncodeInternal(spatialSimplify(geometry, tolerance)), nil

	case "ST_LINEINTERPOLATEPOINT":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if geometry.typeID != 2 {
			return nil, fmt.Errorf("LineString geometry expected")
		}
		fraction, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		if math.IsNaN(fraction) || math.IsInf(fraction, 0) || fraction < 0 || fraction > 1 {
			return nil, fmt.Errorf("fraction is out of range")
		}
		point, err := spatialPointAtDistance(geometry, spatialLength(geometry)*fraction)
		if err != nil {
			return nil, err
		}
		return spatialEncodeInternal(point), nil

	case "ST_LINEINTERPOLATEPOINTS":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if geometry.typeID != 2 {
			return nil, fmt.Errorf("LineString geometry expected")
		}
		fraction, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		if math.IsNaN(fraction) || math.IsInf(fraction, 0) || fraction <= 0 || fraction > 1 {
			return nil, fmt.Errorf("fraction is out of range")
		}
		points := []spatialGeometry{{srid: geometry.srid, typeID: 1, points: []spatialPoint{geometry.points[0]}}}
		for multiplier := 1.0; multiplier*fraction <= 1.0+1e-12; multiplier++ {
			point, pointErr := spatialPointAtDistance(geometry, spatialLength(geometry)*math.Min(1, multiplier*fraction))
			if pointErr != nil {
				return nil, pointErr
			}
			points = append(points, point)
		}
		return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 4, children: points}), nil

	case "ST_POINTATDISTANCE", "POINTATDISTANCE":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		distance, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		point, err := spatialPointAtDistance(geometry, distance)
		if err != nil {
			return nil, err
		}
		return spatialEncodeInternal(point), nil

	case "ST_GEOHASH":
		if len(args) != 2 && len(args) != 3 {
			return nil, nil
		}
		var longitude, latitude float64
		if len(args) == 3 {
			var err error
			longitude, err = numericArg(args[0])
			if err != nil {
				return nil, err
			}
			latitude, err = numericArg(args[1])
			if err != nil {
				return nil, err
			}
		} else {
			geometry, err := spatialPointArgument([]interface{}{args[0]})
			if err != nil {
				return nil, err
			}
			longitude, latitude = geometry.points[0].x, geometry.points[0].y
		}
		maxLength, err := integerArg(args[len(args)-1])
		if err != nil || maxLength < 1 || maxLength > 100 {
			return nil, fmt.Errorf("invalid GeoHash length")
		}
		return spatialEncodeGeoHash(longitude, latitude, int(maxLength))

	case "ST_LATFROMGEOHASH", "ST_LONGFROMGEOHASH":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		longitude, latitude, err := spatialDecodeGeoHash(compatString(args[0]))
		if err != nil {
			return nil, err
		}
		if name == "ST_LATFROMGEOHASH" {
			return latitude, nil
		}
		return longitude, nil

	case "ST_POINTFROMGEOHASH":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		longitude, latitude, err := spatialDecodeGeoHash(compatString(args[0]))
		if err != nil {
			return nil, err
		}
		srid, err := integerArg(args[1])
		if err != nil || srid < 0 || srid > math.MaxUint32 {
			return nil, fmt.Errorf("invalid geometry SRID")
		}
		return spatialEncodeInternal(spatialGeometry{srid: uint32(srid), typeID: 1, points: []spatialPoint{{x: longitude, y: latitude}}}), nil

	case "ST_LATITUDE", "LATITUDE", "ST_LONGITUDE", "LONGITUDE":
		geometry, err := spatialPointArgument(args)
		if err != nil || geometry == nil {
			return nil, err
		}
		if strings.Contains(name, "LATITUDE") {
			return geometry.points[0].y, nil
		}
		return geometry.points[0].x, nil

	case "ST_SWAPXY", "SWAPXY":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		spatialSwapXY(&geometry)
		return spatialEncodeInternal(geometry), nil

	case "ST_X", "X":
		geometry, err := spatialPointArgument(args)
		if err != nil || geometry == nil {
			return nil, err
		}
		return geometry.points[0].x, nil

	case "ST_Y", "Y":
		geometry, err := spatialPointArgument(args)
		if err != nil || geometry == nil {
			return nil, err
		}
		return geometry.points[0].y, nil

	case "ST_ISEMPTY", "ISEMPTY":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return int64(boolToInt(spatialGeometryEmpty(geometry))), nil

	case "ST_ISVALID", "ISVALID":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return int64(boolToInt(spatialGeometryValid(geometry))), nil

	case "ST_VALIDATE", "VALIDATE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		if !spatialGeometryValid(geometry) {
			return nil, nil
		}
		return spatialEncodeInternal(geometry), nil

	case "ST_ISSIMPLE", "ISSIMPLE", "ST_IS_SIMPLE", "IS_SIMPLE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		return int64(boolToInt(spatialGeometrySimple(geometry))), nil

	case "ST_POINTONSURFACE", "POINTONSURFACE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		geometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		point, ok := spatialPointOnSurface(geometry)
		if !ok {
			return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 1}), nil
		}
		return spatialEncodeInternal(spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{point}}), nil

	case "ST_EQUALS", "ST_CONTAINS", "ST_WITHIN", "ST_INTERSECTS", "ST_DISJOINT", "ST_TOUCHES", "ST_OVERLAPS", "ST_CROSSES", "MBRCONTAINS", "MBRWITHIN", "MBRINTERSECTS", "MBREQUALS", "MBRDISJOINT":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		right, err := spatialDecodeArgument(args[1])
		if err != nil {
			return nil, err
		}
		result, err := spatialPredicate(name, left, right)
		if err != nil {
			return nil, err
		}
		return int64(boolToInt(result)), nil

	case "ST_DISTANCE", "ST_DISTANCE_SPHERE":
		if (len(args) != 2 && len(args) != 3) || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		leftGeometry, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		rightGeometry, err := spatialDecodeArgument(args[1])
		if err != nil {
			return nil, err
		}
		if leftGeometry.srid != rightGeometry.srid {
			return nil, fmt.Errorf("spatial reference systems do not match")
		}
		if name == "ST_DISTANCE_SPHERE" {
			radius := 6370986.0
			if len(args) == 3 {
				radius, err = numericArg(args[2])
				if err != nil {
					return nil, err
				}
				if radius <= 0 || math.IsNaN(radius) || math.IsInf(radius, 0) {
					return nil, fmt.Errorf("radius must be greater than zero")
				}
			}
			distance, ok, distanceErr := spatialSphereGeometryDistance(leftGeometry, rightGeometry, radius)
			if distanceErr != nil {
				return float64(0), distanceErr
			}
			if !ok {
				return nil, nil
			}
			return distance, nil
		}
		distance, ok := spatialGeometryDistance(leftGeometry, rightGeometry)
		if !ok {
			return nil, nil
		}
		return distance, nil

	case "ST_HAUSDORFFDISTANCE", "ST_FRECHETDISTANCE":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := spatialDecodeArgument(args[0])
		if err != nil {
			return nil, err
		}
		right, err := spatialDecodeArgument(args[1])
		if err != nil {
			return nil, err
		}
		if left.srid != right.srid {
			return nil, fmt.Errorf("spatial reference systems do not match")
		}
		if name == "ST_FRECHETDISTANCE" && (left.typeID != 2 || right.typeID != 2) {
			return nil, fmt.Errorf("LineString geometry expected")
		}
		leftPoints, rightPoints := make([]spatialPoint, 0), make([]spatialPoint, 0)
		spatialCollectPoints(left, &leftPoints)
		spatialCollectPoints(right, &rightPoints)
		if len(leftPoints) == 0 || len(rightPoints) == 0 {
			return nil, nil
		}
		if name == "ST_HAUSDORFFDISTANCE" {
			return spatialDiscreteHausdorffDistance(leftPoints, rightPoints), nil
		}
		return spatialDiscreteFrechetDistance(leftPoints, rightPoints), nil
	default:
		return nil, fmt.Errorf("unknown spatial function: %s", name)
	}
}

func spatialDiscreteHausdorffDistance(left, right []spatialPoint) float64 {
	directed := func(source, target []spatialPoint) float64 {
		maximum := 0.0
		for _, point := range source {
			minimum := math.Inf(1)
			for _, candidate := range target {
				minimum = math.Min(minimum, math.Hypot(point.x-candidate.x, point.y-candidate.y))
			}
			maximum = math.Max(maximum, minimum)
		}
		return maximum
	}
	return math.Max(directed(left, right), directed(right, left))
}

func spatialDiscreteFrechetDistance(left, right []spatialPoint) float64 {
	values := make([][]float64, len(left))
	for index := range values {
		values[index] = make([]float64, len(right))
		for candidate := range values[index] {
			values[index][candidate] = -1
		}
	}
	var visit func(int, int) float64
	visit = func(leftIndex, rightIndex int) float64 {
		if values[leftIndex][rightIndex] >= 0 {
			return values[leftIndex][rightIndex]
		}
		distance := math.Hypot(left[leftIndex].x-right[rightIndex].x, left[leftIndex].y-right[rightIndex].y)
		if leftIndex == 0 && rightIndex == 0 {
			values[leftIndex][rightIndex] = distance
		} else if leftIndex == 0 {
			values[leftIndex][rightIndex] = math.Max(visit(leftIndex, rightIndex-1), distance)
		} else if rightIndex == 0 {
			values[leftIndex][rightIndex] = math.Max(visit(leftIndex-1, rightIndex), distance)
		} else {
			values[leftIndex][rightIndex] = math.Max(math.Min(math.Min(visit(leftIndex-1, rightIndex), visit(leftIndex-1, rightIndex-1)), visit(leftIndex, rightIndex-1)), distance)
		}
		return values[leftIndex][rightIndex]
	}
	return visit(len(left)-1, len(right)-1)
}

func spatialDistanceSphere(left, right spatialPoint) (float64, error) {
	if left.x < -180 || left.x > 180 || right.x < -180 || right.x > 180 ||
		left.y < -90 || left.y > 90 || right.y < -90 || right.y > 90 {
		return 0, fmt.Errorf("longitude/latitude is out of range")
	}
	const earthRadiusMeters = 6370986.0
	degreesToRadians := math.Pi / 180
	lat1, lat2 := left.y*degreesToRadians, right.y*degreesToRadians
	dLat := (right.y - left.y) * degreesToRadians
	dLon := (right.x - left.x) * degreesToRadians
	haversine := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	if haversine > 1 {
		haversine = 1
	}
	return earthRadiusMeters * 2 * math.Asin(math.Sqrt(haversine)), nil
}

type spatialSphereVector struct {
	x float64
	y float64
	z float64
}

func spatialSphereVectorForPoint(point spatialPoint) (spatialSphereVector, error) {
	if math.IsNaN(point.x) || math.IsNaN(point.y) {
		return spatialSphereVector{}, nil
	}
	if math.IsInf(point.x, 0) || math.IsInf(point.y, 0) ||
		point.x < -180 || point.x > 180 || point.y < -90 || point.y > 90 {
		return spatialSphereVector{}, fmt.Errorf("longitude/latitude is out of range")
	}
	degreesToRadians := math.Pi / 180
	longitude, latitude := point.x*degreesToRadians, point.y*degreesToRadians
	cosLatitude := math.Cos(latitude)
	return spatialSphereVector{
		x: cosLatitude * math.Cos(longitude),
		y: cosLatitude * math.Sin(longitude),
		z: math.Sin(latitude),
	}, nil
}

func spatialSphereVectorNorm(value spatialSphereVector) float64 {
	return math.Sqrt(value.x*value.x + value.y*value.y + value.z*value.z)
}

func spatialSphereVectorDot(left, right spatialSphereVector) float64 {
	return left.x*right.x + left.y*right.y + left.z*right.z
}

func spatialSphereVectorCross(left, right spatialSphereVector) spatialSphereVector {
	return spatialSphereVector{
		x: left.y*right.z - left.z*right.y,
		y: left.z*right.x - left.x*right.z,
		z: left.x*right.y - left.y*right.x,
	}
}

func spatialSphereVectorScale(value spatialSphereVector, scale float64) spatialSphereVector {
	return spatialSphereVector{x: value.x * scale, y: value.y * scale, z: value.z * scale}
}

func spatialSphereVectorSubtract(left, right spatialSphereVector) spatialSphereVector {
	return spatialSphereVector{x: left.x - right.x, y: left.y - right.y, z: left.z - right.z}
}

func spatialSphereVectorNormalize(value spatialSphereVector) (spatialSphereVector, bool) {
	norm := spatialSphereVectorNorm(value)
	if norm < 1e-15 {
		return spatialSphereVector{}, false
	}
	return spatialSphereVectorScale(value, 1/norm), true
}

func spatialSphereAngularDistance(left, right spatialSphereVector) float64 {
	crossNorm := spatialSphereVectorNorm(spatialSphereVectorCross(left, right))
	dot := spatialSphereVectorDot(left, right)
	if dot > 1 {
		dot = 1
	} else if dot < -1 {
		dot = -1
	}
	return math.Atan2(crossNorm, dot)
}

func spatialSpherePointOnArc(point, start, end spatialSphereVector) bool {
	arcAngle := spatialSphereAngularDistance(start, end)
	return spatialSphereAngularDistance(start, point)+spatialSphereAngularDistance(point, end) <= arcAngle+1e-10
}

func spatialSpherePointToArcDistance(point, start, end spatialPoint) (float64, bool, error) {
	pointVector, err := spatialSphereVectorForPoint(point)
	if err != nil {
		return 0, false, err
	}
	startVector, err := spatialSphereVectorForPoint(start)
	if err != nil {
		return 0, false, err
	}
	endVector, err := spatialSphereVectorForPoint(end)
	if err != nil {
		return 0, false, err
	}
	if spatialSphereVectorNorm(pointVector) == 0 || spatialSphereVectorNorm(startVector) == 0 || spatialSphereVectorNorm(endVector) == 0 {
		return 0, false, nil
	}
	endpointMinimum := math.Min(spatialSphereAngularDistance(pointVector, startVector), spatialSphereAngularDistance(pointVector, endVector))
	arcNormal, ok := spatialSphereVectorNormalize(spatialSphereVectorCross(startVector, endVector))
	if !ok {
		return endpointMinimum, true, nil
	}
	projection := spatialSphereVectorSubtract(pointVector, spatialSphereVectorScale(arcNormal, spatialSphereVectorDot(pointVector, arcNormal)))
	projected, ok := spatialSphereVectorNormalize(projection)
	if !ok {
		return math.Min(endpointMinimum, math.Pi/2), true, nil
	}
	minimum := endpointMinimum
	for _, candidate := range []spatialSphereVector{projected, spatialSphereVectorScale(projected, -1)} {
		if spatialSpherePointOnArc(candidate, startVector, endVector) {
			minimum = math.Min(minimum, spatialSphereAngularDistance(pointVector, candidate))
		}
	}
	return minimum, true, nil
}

func spatialSphereArcsIntersect(leftStart, leftEnd, rightStart, rightEnd spatialPoint) (bool, error) {
	leftStartVector, err := spatialSphereVectorForPoint(leftStart)
	if err != nil {
		return false, err
	}
	leftEndVector, err := spatialSphereVectorForPoint(leftEnd)
	if err != nil {
		return false, err
	}
	rightStartVector, err := spatialSphereVectorForPoint(rightStart)
	if err != nil {
		return false, err
	}
	rightEndVector, err := spatialSphereVectorForPoint(rightEnd)
	if err != nil {
		return false, err
	}
	if spatialSphereVectorNorm(leftStartVector) == 0 || spatialSphereVectorNorm(leftEndVector) == 0 ||
		spatialSphereVectorNorm(rightStartVector) == 0 || spatialSphereVectorNorm(rightEndVector) == 0 {
		return false, nil
	}
	leftNormal, leftOK := spatialSphereVectorNormalize(spatialSphereVectorCross(leftStartVector, leftEndVector))
	rightNormal, rightOK := spatialSphereVectorNormalize(spatialSphereVectorCross(rightStartVector, rightEndVector))
	if !leftOK || !rightOK {
		return spatialSphereAngularDistance(leftStartVector, rightStartVector) < 1e-10 ||
			spatialSphereAngularDistance(leftStartVector, rightEndVector) < 1e-10 ||
			spatialSphereAngularDistance(leftEndVector, rightStartVector) < 1e-10 ||
			spatialSphereAngularDistance(leftEndVector, rightEndVector) < 1e-10, nil
	}
	intersection, ok := spatialSphereVectorNormalize(spatialSphereVectorCross(leftNormal, rightNormal))
	if !ok {
		return spatialSpherePointOnArc(leftStartVector, rightStartVector, rightEndVector) ||
			spatialSpherePointOnArc(leftEndVector, rightStartVector, rightEndVector) ||
			spatialSpherePointOnArc(rightStartVector, leftStartVector, leftEndVector) ||
			spatialSpherePointOnArc(rightEndVector, leftStartVector, leftEndVector), nil
	}
	return (spatialSpherePointOnArc(intersection, leftStartVector, leftEndVector) && spatialSpherePointOnArc(intersection, rightStartVector, rightEndVector)) ||
		(spatialSpherePointOnArc(spatialSphereVectorScale(intersection, -1), leftStartVector, leftEndVector) && spatialSpherePointOnArc(spatialSphereVectorScale(intersection, -1), rightStartVector, rightEndVector)), nil
}

func spatialSphereGeometryPoints(geometry spatialGeometry) ([]spatialPoint, error) {
	points := make([]spatialPoint, 0)
	spatialCollectPoints(geometry, &points)
	valid := make([]spatialPoint, 0, len(points))
	for _, point := range points {
		vector, err := spatialSphereVectorForPoint(point)
		if err != nil {
			return nil, err
		}
		if spatialSphereVectorNorm(vector) != 0 {
			valid = append(valid, point)
		}
	}
	return valid, nil
}

func spatialSphereGeometryContainsPoint(geometry spatialGeometry, point spatialPoint) bool {
	switch geometry.typeID {
	case 3:
		return spatialPolygonCoversPoint(geometry, point)
	default:
		for _, child := range geometry.children {
			if spatialSphereGeometryContainsPoint(child, point) {
				return true
			}
		}
		return false
	}
}

func spatialSphereGeometryDistance(left, right spatialGeometry, radius float64) (float64, bool, error) {
	leftPoints, err := spatialSphereGeometryPoints(left)
	if err != nil {
		return 0, false, err
	}
	rightPoints, err := spatialSphereGeometryPoints(right)
	if err != nil {
		return 0, false, err
	}
	if len(leftPoints) == 0 || len(rightPoints) == 0 {
		return 0, false, nil
	}
	for _, point := range leftPoints {
		if spatialSphereGeometryContainsPoint(right, point) {
			return 0, true, nil
		}
	}
	for _, point := range rightPoints {
		if spatialSphereGeometryContainsPoint(left, point) {
			return 0, true, nil
		}
	}
	leftSegments, rightSegments := spatialGeometrySegments(left), spatialGeometrySegments(right)
	minimum := math.Inf(1)
	for _, leftPoint := range leftPoints {
		leftVector, _ := spatialSphereVectorForPoint(leftPoint)
		for _, rightPoint := range rightPoints {
			rightVector, _ := spatialSphereVectorForPoint(rightPoint)
			minimum = math.Min(minimum, spatialSphereAngularDistance(leftVector, rightVector))
		}
		for _, segment := range rightSegments {
			distance, ok, distanceErr := spatialSpherePointToArcDistance(leftPoint, segment[0], segment[1])
			if distanceErr != nil {
				return 0, false, distanceErr
			}
			if ok {
				minimum = math.Min(minimum, distance)
			}
		}
	}
	for _, rightPoint := range rightPoints {
		for _, segment := range leftSegments {
			distance, ok, distanceErr := spatialSpherePointToArcDistance(rightPoint, segment[0], segment[1])
			if distanceErr != nil {
				return 0, false, distanceErr
			}
			if ok {
				minimum = math.Min(minimum, distance)
			}
		}
	}
	for _, leftSegment := range leftSegments {
		for _, rightSegment := range rightSegments {
			intersects, distanceErr := spatialSphereArcsIntersect(leftSegment[0], leftSegment[1], rightSegment[0], rightSegment[1])
			if distanceErr != nil {
				return 0, false, distanceErr
			}
			if intersects {
				return 0, true, nil
			}
		}
	}
	if math.IsInf(minimum, 1) {
		return 0, false, nil
	}
	return minimum * radius, true, nil
}

const spatialGeoHashAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

func spatialEncodeGeoHash(longitude, latitude float64, maxLength int) (string, error) {
	if longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90 {
		return "", fmt.Errorf("longitude/latitude is out of range")
	}
	if maxLength < 1 || maxLength > 100 {
		return "", fmt.Errorf("invalid GeoHash length")
	}
	lonRange, latRange := [2]float64{-180, 180}, [2]float64{-90, 90}
	evenBit := true
	bitValue, character := 0, 0
	encoded := strings.Builder{}
	encoded.Grow(maxLength)
	for encoded.Len() < maxLength {
		var value, midpoint float64
		if evenBit {
			value, midpoint = longitude, (lonRange[0]+lonRange[1])/2
		} else {
			value, midpoint = latitude, (latRange[0]+latRange[1])/2
		}
		if value >= midpoint {
			character |= 1 << (4 - bitValue)
			if evenBit {
				lonRange[0] = midpoint
			} else {
				latRange[0] = midpoint
			}
		} else if evenBit {
			lonRange[1] = midpoint
		} else {
			latRange[1] = midpoint
		}
		evenBit = !evenBit
		if bitValue < 4 {
			bitValue++
		} else {
			encoded.WriteByte(spatialGeoHashAlphabet[character])
			bitValue, character = 0, 0
		}
	}
	return encoded.String(), nil
}

func spatialDecodeGeoHash(value string) (float64, float64, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return 0, 0, fmt.Errorf("invalid GeoHash")
	}
	if len(value) > 433 {
		value = value[:433]
	}
	lonRange, latRange := [2]float64{-180, 180}, [2]float64{-90, 90}
	evenBit := true
	for index := 0; index < len(value); index++ {
		character := strings.IndexByte(spatialGeoHashAlphabet, value[index])
		if character < 0 {
			return 0, 0, fmt.Errorf("invalid GeoHash")
		}
		for mask := 16; mask > 0; mask >>= 1 {
			if evenBit {
				midpoint := (lonRange[0] + lonRange[1]) / 2
				if character&mask != 0 {
					lonRange[0] = midpoint
				} else {
					lonRange[1] = midpoint
				}
			} else {
				midpoint := (latRange[0] + latRange[1]) / 2
				if character&mask != 0 {
					latRange[0] = midpoint
				} else {
					latRange[1] = midpoint
				}
			}
			evenBit = !evenBit
		}
	}
	return (lonRange[0] + lonRange[1]) / 2, (latRange[0] + latRange[1]) / 2, nil
}

func spatialPointBuffer(geometry spatialGeometry, distance float64) spatialGeometry {
	const segments = 32
	center := geometry.points[0]
	ring := make([]spatialPoint, 0, segments+1)
	for index := 0; index <= segments; index++ {
		angle := 2 * math.Pi * float64(index) / segments
		ring = append(ring, spatialPoint{
			x: center.x + distance*math.Cos(angle),
			y: center.y + distance*math.Sin(angle),
		})
	}
	return spatialGeometry{srid: geometry.srid, typeID: 3, rings: [][]spatialPoint{ring}}
}

func spatialLineBuffer(geometry spatialGeometry, distance float64) spatialGeometry {
	const arcSegments = 16
	left, right := geometry.points[0], geometry.points[1]
	deltaX, deltaY := right.x-left.x, right.y-left.y
	length := math.Hypot(deltaX, deltaY)
	if length == 0 {
		return spatialPointBuffer(spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{left}}, distance)
	}
	normalX, normalY := -deltaY/length, deltaX/length
	startAngle := math.Atan2(normalY, normalX)
	ring := make([]spatialPoint, 0, arcSegments*2+3)
	ring = append(ring, spatialPoint{x: left.x + normalX*distance, y: left.y + normalY*distance})
	ring = append(ring, spatialPoint{x: right.x + normalX*distance, y: right.y + normalY*distance})
	for index := 1; index <= arcSegments; index++ {
		angle := startAngle - math.Pi*float64(index)/arcSegments
		ring = append(ring, spatialPoint{x: right.x + distance*math.Cos(angle), y: right.y + distance*math.Sin(angle)})
	}
	for index := 1; index <= arcSegments; index++ {
		angle := startAngle - math.Pi - math.Pi*float64(index)/arcSegments
		ring = append(ring, spatialPoint{x: left.x + distance*math.Cos(angle), y: left.y + distance*math.Sin(angle)})
	}
	return spatialGeometry{srid: geometry.srid, typeID: 3, rings: [][]spatialPoint{append(ring, ring[0])}}
}

func spatialIntersection(left, right spatialGeometry) (spatialGeometry, bool) {
	empty := spatialGeometry{srid: left.srid, typeID: 7}
	if spatialGeometryEmpty(left) || spatialGeometryEmpty(right) {
		return empty, true
	}
	if left.typeID == 1 && right.typeID == 1 {
		if len(left.points) == 1 && len(right.points) == 1 && left.points[0] == right.points[0] {
			return left, true
		}
		return empty, true
	}
	if left.typeID == 1 {
		if len(left.points) == 1 && spatialGeometryContainsPointOrLine(right, left.points[0]) {
			return left, true
		}
		return empty, true
	}
	if right.typeID == 1 {
		if len(right.points) == 1 && spatialGeometryContainsPointOrLine(left, right.points[0]) {
			return right, true
		}
		return empty, true
	}
	leftBounds, leftOK := spatialAxisAlignedRectangle(left)
	rightBounds, rightOK := spatialAxisAlignedRectangle(right)
	if !leftOK || !rightOK {
		return spatialGeometry{}, false
	}
	minX, maxX := math.Max(leftBounds[0], rightBounds[0]), math.Min(leftBounds[1], rightBounds[1])
	minY, maxY := math.Max(leftBounds[2], rightBounds[2]), math.Min(leftBounds[3], rightBounds[3])
	const epsilon = 1e-12
	switch {
	case maxX-minX > epsilon && maxY-minY > epsilon:
		return spatialGeometry{srid: left.srid, typeID: 3, rings: [][]spatialPoint{{
			{x: minX, y: minY}, {x: maxX, y: minY}, {x: maxX, y: maxY}, {x: minX, y: maxY}, {x: minX, y: minY},
		}}}, true
	case math.Abs(maxX-minX) <= epsilon && maxY-minY > epsilon:
		return spatialGeometry{srid: left.srid, typeID: 2, points: []spatialPoint{{x: minX, y: minY}, {x: minX, y: maxY}}}, true
	case maxX-minX > epsilon && math.Abs(maxY-minY) <= epsilon:
		return spatialGeometry{srid: left.srid, typeID: 2, points: []spatialPoint{{x: minX, y: minY}, {x: maxX, y: minY}}}, true
	case math.Abs(maxX-minX) <= epsilon && math.Abs(maxY-minY) <= epsilon:
		return spatialGeometry{srid: left.srid, typeID: 1, points: []spatialPoint{{x: minX, y: minY}}}, true
	default:
		return empty, true
	}
}

func spatialAxisAlignedRectangle(geometry spatialGeometry) ([4]float64, bool) {
	var bounds [4]float64
	if geometry.typeID != 3 || len(geometry.rings) != 1 || len(geometry.rings[0]) != 5 {
		return bounds, false
	}
	ring := geometry.rings[0]
	if ring[0] != ring[4] {
		return bounds, false
	}
	minX, maxX := ring[0].x, ring[0].x
	minY, maxY := ring[0].y, ring[0].y
	for index := 0; index < 4; index++ {
		minX = math.Min(minX, ring[index].x)
		maxX = math.Max(maxX, ring[index].x)
		minY = math.Min(minY, ring[index].y)
		maxY = math.Max(maxY, ring[index].y)
		deltaX := ring[index+1].x - ring[index].x
		deltaY := ring[index+1].y - ring[index].y
		if deltaX != 0 && deltaY != 0 {
			return bounds, false
		}
	}
	if minX == maxX || minY == maxY {
		return bounds, false
	}
	bounds = [4]float64{minX, maxX, minY, maxY}
	return bounds, true
}

func spatialUnion(left, right spatialGeometry) (spatialGeometry, bool) {
	if spatialGeometryEmpty(left) {
		return right, true
	}
	if spatialGeometryEmpty(right) {
		return left, true
	}
	if spatialGeometryEqual(left, right) {
		return left, true
	}
	if left.typeID == 1 && right.typeID == 1 && len(left.points) == 1 && len(right.points) == 1 {
		return spatialGeometry{srid: left.srid, typeID: 4, children: []spatialGeometry{left, right}}, true
	}
	if left.typeID == 1 && spatialGeometryContainsPointOrLine(right, left.points[0]) {
		return right, true
	}
	if right.typeID == 1 && spatialGeometryContainsPointOrLine(left, right.points[0]) {
		return left, true
	}
	leftBounds, leftOK := spatialAxisAlignedRectangle(left)
	rightBounds, rightOK := spatialAxisAlignedRectangle(right)
	if !leftOK || !rightOK {
		return spatialGeometry{}, false
	}
	if spatialGeometryContains(left, right) {
		return left, true
	}
	if spatialGeometryContains(right, left) {
		return right, true
	}
	if leftBounds[1] < rightBounds[0] || rightBounds[1] < leftBounds[0] || leftBounds[3] < rightBounds[2] || rightBounds[3] < leftBounds[2] {
		return spatialGeometry{srid: left.srid, typeID: 6, children: []spatialGeometry{left, right}}, true
	}
	return spatialGeometry{}, false
}

func spatialDifference(left, right spatialGeometry) (spatialGeometry, bool) {
	if spatialGeometryEmpty(left) {
		return left, true
	}
	if spatialGeometryEqual(left, right) {
		return spatialGeometry{srid: left.srid, typeID: 7}, true
	}
	if left.typeID == 1 {
		if len(left.points) != 1 {
			return left, true
		}
		if right.typeID == 1 {
			return left, true
		}
		if spatialGeometryContainsPointOrLine(right, left.points[0]) {
			return spatialGeometry{srid: left.srid, typeID: 1}, true
		}
		if right.typeID == 3 {
			if _, ok := spatialAxisAlignedRectangle(right); ok {
				return left, true
			}
		}
		return spatialGeometry{}, false
	}
	if right.typeID == 1 {
		if _, ok := spatialAxisAlignedRectangle(left); ok {
			return left, true
		}
		return spatialGeometry{}, false
	}
	if _, leftOK := spatialAxisAlignedRectangle(left); leftOK {
		if _, rightOK := spatialAxisAlignedRectangle(right); rightOK {
			return spatialGeometry{}, false
		}
	}
	return spatialGeometry{}, false
}

func spatialSymmetricDifference(left, right spatialGeometry) (spatialGeometry, bool) {
	if left.typeID != 1 || right.typeID != 1 || len(left.points) != 1 || len(right.points) != 1 {
		return spatialGeometry{}, false
	}
	if left.points[0] == right.points[0] {
		return spatialGeometry{srid: left.srid, typeID: 7}, true
	}
	return spatialGeometry{srid: left.srid, typeID: 4, children: []spatialGeometry{left, right}}, true
}

func spatialPointArgument(args []interface{}) (*spatialGeometry, error) {
	if len(args) != 1 || args[0] == nil {
		return nil, nil
	}
	geometry, err := spatialDecodeArgument(args[0])
	if err != nil {
		return nil, err
	}
	if geometry.typeID != 1 || len(geometry.points) != 1 {
		return nil, fmt.Errorf("point geometry expected")
	}
	return &geometry, nil
}

func spatialDimension(geometry spatialGeometry) uint32 {
	switch geometry.typeID {
	case 1:
		return 0
	case 2, 4, 5:
		return 1
	case 3, 6:
		return 2
	case 7:
		var dimension uint32
		for _, child := range geometry.children {
			if childDimension := spatialDimension(child); childDimension > dimension {
				dimension = childDimension
			}
		}
		return dimension
	default:
		return 0
	}
}

func spatialPointCount(geometry spatialGeometry) int {
	count := len(geometry.points)
	for _, ring := range geometry.rings {
		count += len(ring)
	}
	for _, child := range geometry.children {
		count += spatialPointCount(child)
	}
	return count
}

func spatialGeometryClosed(geometry spatialGeometry) bool {
	switch geometry.typeID {
	case 2:
		return len(geometry.points) == 0 || len(geometry.points) > 1 && geometry.points[0] == geometry.points[len(geometry.points)-1]
	case 5:
		for _, child := range geometry.children {
			if !spatialGeometryClosed(child) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func spatialArea(geometry spatialGeometry) float64 {
	switch geometry.typeID {
	case 3:
		if len(geometry.rings) == 0 {
			return 0
		}
		area := math.Abs(spatialRingSignedArea(geometry.rings[0]))
		for _, hole := range geometry.rings[1:] {
			area -= math.Abs(spatialRingSignedArea(hole))
		}
		if area < 0 {
			return 0
		}
		return area
	case 6:
		area := 0.0
		for _, child := range geometry.children {
			area += spatialArea(child)
		}
		return area
	case 7:
		area := 0.0
		for _, child := range geometry.children {
			area += spatialArea(child)
		}
		return area
	default:
		return 0
	}
}

func spatialRingSignedArea(ring []spatialPoint) float64 {
	if len(ring) < 3 {
		return 0
	}
	area := 0.0
	for index, point := range ring {
		next := ring[(index+1)%len(ring)]
		area += point.x*next.y - next.x*point.y
	}
	return area / 2
}

func spatialLength(geometry spatialGeometry) float64 {
	switch geometry.typeID {
	case 2:
		return spatialPointSequenceLength(geometry.points)
	case 5:
		length := 0.0
		for _, child := range geometry.children {
			length += spatialLength(child)
		}
		return length
	case 7:
		length := 0.0
		for _, child := range geometry.children {
			length += spatialLength(child)
		}
		return length
	default:
		return 0
	}
}

func spatialPointAtDistance(geometry spatialGeometry, distance float64) (spatialGeometry, error) {
	if geometry.typeID != 2 {
		return spatialGeometry{}, fmt.Errorf("LineString geometry expected")
	}
	if math.IsNaN(distance) || math.IsInf(distance, 0) {
		return spatialGeometry{}, fmt.Errorf("distance is out of range")
	}
	total := spatialLength(geometry)
	if distance < 0 || distance > total {
		return spatialGeometry{}, fmt.Errorf("distance is out of range")
	}
	if len(geometry.points) < 2 {
		return spatialGeometry{}, fmt.Errorf("LineString geometry is not well-formed")
	}
	remaining := distance
	for index := 1; index < len(geometry.points); index++ {
		start, end := geometry.points[index-1], geometry.points[index]
		segmentLength := math.Hypot(end.x-start.x, end.y-start.y)
		if remaining <= segmentLength || index == len(geometry.points)-1 {
			if segmentLength == 0 {
				return spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{start}}, nil
			}
			ratio := remaining / segmentLength
			point := spatialPoint{x: start.x + (end.x-start.x)*ratio, y: start.y + (end.y-start.y)*ratio}
			return spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{point}}, nil
		}
		remaining -= segmentLength
	}
	return spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{geometry.points[len(geometry.points)-1]}}, nil
}

func spatialPointSequenceLength(points []spatialPoint) float64 {
	length := 0.0
	for index := 1; index < len(points); index++ {
		length += math.Hypot(points[index].x-points[index-1].x, points[index].y-points[index-1].y)
	}
	return length
}

func spatialCentroid(geometry spatialGeometry) (spatialPoint, bool) {
	switch geometry.typeID {
	case 1:
		if len(geometry.points) == 1 {
			return geometry.points[0], true
		}
	case 2:
		return spatialLineCentroid(geometry.points)
	case 3:
		return spatialPolygonCentroid(geometry.rings)
	case 4, 5, 6, 7:
		var sum spatialPoint
		count := 0
		for _, child := range geometry.children {
			point, ok := spatialCentroid(child)
			if ok {
				sum.x += point.x
				sum.y += point.y
				count++
			}
		}
		if count > 0 {
			return spatialPoint{x: sum.x / float64(count), y: sum.y / float64(count)}, true
		}
	}
	return spatialPoint{}, false
}

func spatialLineCentroid(points []spatialPoint) (spatialPoint, bool) {
	if len(points) == 0 {
		return spatialPoint{}, false
	}
	if len(points) == 1 {
		return points[0], true
	}
	var sum spatialPoint
	total := 0.0
	for index := 1; index < len(points); index++ {
		segment := math.Hypot(points[index].x-points[index-1].x, points[index].y-points[index-1].y)
		if segment == 0 {
			continue
		}
		sum.x += (points[index].x + points[index-1].x) / 2 * segment
		sum.y += (points[index].y + points[index-1].y) / 2 * segment
		total += segment
	}
	if total == 0 {
		return points[0], true
	}
	return spatialPoint{x: sum.x / total, y: sum.y / total}, true
}

func spatialPolygonCentroid(rings [][]spatialPoint) (spatialPoint, bool) {
	if len(rings) == 0 || len(rings[0]) < 3 {
		return spatialPoint{}, false
	}
	var sum spatialPoint
	totalArea := 0.0
	for ringIndex, ring := range rings {
		area := spatialRingSignedArea(ring)
		if area == 0 {
			continue
		}
		factor := 1.0
		if ringIndex > 0 {
			factor = -1
		}
		weightedArea := area * factor
		centroidFactor := 1 / (6 * area)
		var x, y float64
		for index, point := range ring {
			next := ring[(index+1)%len(ring)]
			cross := point.x*next.y - next.x*point.y
			x += (point.x + next.x) * cross
			y += (point.y + next.y) * cross
		}
		sum.x += x * centroidFactor * weightedArea
		sum.y += y * centroidFactor * weightedArea
		totalArea += weightedArea
	}
	if totalArea == 0 {
		return spatialPoint{}, false
	}
	return spatialPoint{x: sum.x / totalArea, y: sum.y / totalArea}, true
}

func spatialEnvelope(geometry spatialGeometry) spatialGeometry {
	points := make([]spatialPoint, 0)
	spatialCollectPoints(geometry, &points)
	if len(points) == 0 {
		return spatialGeometry{srid: geometry.srid, typeID: 7}
	}
	minX, minY := points[0].x, points[0].y
	maxX, maxY := minX, minY
	for _, point := range points[1:] {
		minX, minY = math.Min(minX, point.x), math.Min(minY, point.y)
		maxX, maxY = math.Max(maxX, point.x), math.Max(maxY, point.y)
	}
	if minX == maxX && minY == maxY {
		return spatialGeometry{srid: geometry.srid, typeID: 1, points: []spatialPoint{{x: minX, y: minY}}}
	}
	return spatialGeometry{srid: geometry.srid, typeID: 3, rings: [][]spatialPoint{{
		{x: minX, y: minY}, {x: maxX, y: minY}, {x: maxX, y: maxY}, {x: minX, y: maxY}, {x: minX, y: minY},
	}}}
}

// spatialConvexHull returns the convex hull of the coordinates represented by
// a geometry. It uses the monotonic-chain algorithm, which is deterministic
// and preserves MySQL's dimensional result convention for the supported 2-D
// compatibility model: a point, line, or polygon depending on the number of
// distinct hull vertices.
func spatialConvexHull(geometry spatialGeometry) spatialGeometry {
	points := make([]spatialPoint, 0)
	spatialCollectPoints(geometry, &points)
	if len(points) == 0 {
		return spatialGeometry{srid: geometry.srid, typeID: 7}
	}

	sort.Slice(points, func(left, right int) bool {
		if points[left].x == points[right].x {
			return points[left].y < points[right].y
		}
		return points[left].x < points[right].x
	})
	unique := points[:0]
	for _, point := range points {
		if len(unique) == 0 || point.x != unique[len(unique)-1].x || point.y != unique[len(unique)-1].y {
			unique = append(unique, point)
		}
	}
	points = unique
	if len(points) == 1 {
		return spatialGeometry{srid: geometry.srid, typeID: 1, points: points}
	}

	cross := func(origin, first, second spatialPoint) float64 {
		return (first.x-origin.x)*(second.y-origin.y) - (first.y-origin.y)*(second.x-origin.x)
	}
	lower := make([]spatialPoint, 0, len(points))
	for _, point := range points {
		for len(lower) >= 2 && cross(lower[len(lower)-2], lower[len(lower)-1], point) <= 0 {
			lower = lower[:len(lower)-1]
		}
		lower = append(lower, point)
	}
	upper := make([]spatialPoint, 0, len(points))
	for index := len(points) - 1; index >= 0; index-- {
		point := points[index]
		for len(upper) >= 2 && cross(upper[len(upper)-2], upper[len(upper)-1], point) <= 0 {
			upper = upper[:len(upper)-1]
		}
		upper = append(upper, point)
	}
	hull := append(lower[:len(lower)-1], upper[:len(upper)-1]...)
	if len(hull) == 2 {
		return spatialGeometry{srid: geometry.srid, typeID: 2, points: hull}
	}
	ring := append(append([]spatialPoint(nil), hull...), hull[0])
	return spatialGeometry{srid: geometry.srid, typeID: 3, rings: [][]spatialPoint{ring}}
}

func spatialSimplify(geometry spatialGeometry, tolerance float64) spatialGeometry {
	simplified := geometry
	simplifySequence := func(points []spatialPoint) []spatialPoint {
		if len(points) <= 2 {
			return append([]spatialPoint(nil), points...)
		}
		keep := make([]bool, len(points))
		keep[0], keep[len(points)-1] = true, true
		var visit func(int, int)
		visit = func(start, end int) {
			if end <= start+1 {
				return
			}
			maximum, split := tolerance, -1
			for index := start + 1; index < end; index++ {
				distance := spatialPointSegmentDistance(points[index], points[start], points[end])
				if distance > maximum {
					maximum, split = distance, index
				}
			}
			if split >= 0 {
				keep[split] = true
				visit(start, split)
				visit(split, end)
			}
		}
		visit(0, len(points)-1)
		result := make([]spatialPoint, 0, len(points))
		for index, point := range points {
			if keep[index] {
				result = append(result, point)
			}
		}
		return result
	}
	simplifyRing := func(ring []spatialPoint) []spatialPoint {
		if len(ring) < 2 || ring[0] != ring[len(ring)-1] {
			return append([]spatialPoint(nil), ring...)
		}
		open := append([]spatialPoint(nil), ring[:len(ring)-1]...)
		if len(open) < 3 {
			return append([]spatialPoint(nil), ring...)
		}
		result := simplifySequence(append(open, open[0]))
		if len(result) < 4 || result[0] != result[len(result)-1] {
			return append([]spatialPoint(nil), ring...)
		}
		return result
	}
	switch geometry.typeID {
	case 2:
		simplified.points = simplifySequence(geometry.points)
		if len(simplified.points) < 2 && len(geometry.points) >= 2 {
			simplified.points = append([]spatialPoint(nil), geometry.points[:2]...)
		}
	case 3:
		simplified.rings = make([][]spatialPoint, len(geometry.rings))
		for index, ring := range geometry.rings {
			simplified.rings[index] = simplifyRing(ring)
		}
	case 5:
		simplified.children = make([]spatialGeometry, len(geometry.children))
		for index, child := range geometry.children {
			simplified.children[index] = spatialSimplify(child, tolerance)
		}
	case 6, 7:
		simplified.children = make([]spatialGeometry, len(geometry.children))
		for index, child := range geometry.children {
			simplified.children[index] = spatialSimplify(child, tolerance)
		}
	}
	return simplified
}

func spatialTransformGeometry(geometry spatialGeometry, targetSRID uint32) (spatialGeometry, bool) {
	result := geometry
	transformPoint := func(point spatialPoint) (spatialPoint, bool) {
		if geometry.srid == 4326 && targetSRID == 3857 {
			if point.x < -180 || point.x > 180 || point.y < -90 || point.y > 90 || math.Abs(point.y) > 85.0511287798066 {
				return spatialPoint{}, false
			}
			const originShift = 20037508.342789244
			x := point.x * originShift / 180
			y := math.Log(math.Tan((90+point.y)*math.Pi/360)) / (math.Pi / 180) * originShift / 180
			return spatialPoint{x: x, y: y}, true
		}
		const originShift = 20037508.342789244
		longitude := point.x / originShift * 180
		latitude := point.y / originShift * 180
		latitude = 180 / math.Pi * (2*math.Atan(math.Exp(latitude*math.Pi/180)) - math.Pi/2)
		if longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90 {
			return spatialPoint{}, false
		}
		return spatialPoint{x: longitude, y: latitude}, true
	}
	transformPoints := func(points []spatialPoint) ([]spatialPoint, bool) {
		result := make([]spatialPoint, len(points))
		for index, point := range points {
			transformed, ok := transformPoint(point)
			if !ok {
				return nil, false
			}
			result[index] = transformed
		}
		return result, true
	}
	points, ok := transformPoints(geometry.points)
	if !ok {
		return spatialGeometry{}, false
	}
	result.points = points
	result.rings = make([][]spatialPoint, len(geometry.rings))
	for index, ring := range geometry.rings {
		transformed, ringOK := transformPoints(ring)
		if !ringOK {
			return spatialGeometry{}, false
		}
		result.rings[index] = transformed
	}
	result.children = make([]spatialGeometry, len(geometry.children))
	for index, child := range geometry.children {
		child.srid = geometry.srid
		transformed, childOK := spatialTransformGeometry(child, targetSRID)
		if !childOK {
			return spatialGeometry{}, false
		}
		result.children[index] = transformed
	}
	result.srid = targetSRID
	return result, true
}

func spatialCollectPoints(geometry spatialGeometry, points *[]spatialPoint) {
	*points = append(*points, geometry.points...)
	for _, ring := range geometry.rings {
		*points = append(*points, ring...)
	}
	for _, child := range geometry.children {
		spatialCollectPoints(child, points)
	}
}

func spatialSwapXY(geometry *spatialGeometry) {
	for index := range geometry.points {
		geometry.points[index].x, geometry.points[index].y = geometry.points[index].y, geometry.points[index].x
	}
	for ringIndex := range geometry.rings {
		for pointIndex := range geometry.rings[ringIndex] {
			geometry.rings[ringIndex][pointIndex].x, geometry.rings[ringIndex][pointIndex].y = geometry.rings[ringIndex][pointIndex].y, geometry.rings[ringIndex][pointIndex].x
		}
	}
	for childIndex := range geometry.children {
		spatialSwapXY(&geometry.children[childIndex])
	}
}

func spatialDecodeArgument(value interface{}) (spatialGeometry, error) {
	if data, ok := spatialBytes(value); ok {
		if geometry, err := spatialDecode(data); err == nil {
			return geometry, nil
		}
		return spatialParseWKT(string(data))
	}
	return spatialParseWKT(compatString(value))
}

func spatialBytes(value interface{}) ([]byte, bool) {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...), true
	case *[]byte:
		if typed != nil {
			return append([]byte(nil), (*typed)...), true
		}
	}
	return nil, false
}

func spatialEncodeInternal(geometry spatialGeometry) []byte {
	wkb := spatialEncodeWKB(geometry)
	internal := make([]byte, 4, len(wkb)+4)
	binary.LittleEndian.PutUint32(internal, geometry.srid)
	return append(internal, wkb...)
}

func spatialEncodeWKB(geometry spatialGeometry) []byte {
	data := []byte{1}
	var typeBytes [4]byte
	binary.LittleEndian.PutUint32(typeBytes[:], geometry.typeID)
	data = append(data, typeBytes[:]...)
	appendPoint := func(point spatialPoint) {
		var raw [16]byte
		binary.LittleEndian.PutUint64(raw[0:8], math.Float64bits(point.x))
		binary.LittleEndian.PutUint64(raw[8:16], math.Float64bits(point.y))
		data = append(data, raw[:]...)
	}
	appendCount := func(count int) {
		var raw [4]byte
		binary.LittleEndian.PutUint32(raw[:], uint32(count))
		data = append(data, raw[:]...)
	}
	switch geometry.typeID {
	case 1:
		if len(geometry.points) == 0 {
			appendPoint(spatialPoint{x: math.NaN(), y: math.NaN()})
		} else {
			appendPoint(geometry.points[0])
		}
	case 2:
		appendCount(len(geometry.points))
		for _, point := range geometry.points {
			appendPoint(point)
		}
	case 3:
		appendCount(len(geometry.rings))
		for _, ring := range geometry.rings {
			appendCount(len(ring))
			for _, point := range ring {
				appendPoint(point)
			}
		}
	case 4, 5, 6, 7:
		appendCount(len(geometry.children))
		for _, child := range geometry.children {
			data = append(data, spatialEncodeWKB(child)...)
		}
	}
	return data
}

func spatialDecode(data []byte) (spatialGeometry, error) {
	if len(data) == 0 {
		return spatialGeometry{}, fmt.Errorf("empty geometry value")
	}
	if geometry, ok := spatialDecodeWKB(data, 0); ok && spatialWKBSize(geometry) == len(data) {
		return geometry, nil
	}
	if len(data) > 4 {
		if geometry, ok := spatialDecodeWKB(data[4:], 0); ok && spatialWKBSize(geometry) == len(data)-4 {
			geometry.srid = binary.LittleEndian.Uint32(data[:4])
			return geometry, nil
		}
	}
	return spatialGeometry{}, fmt.Errorf("invalid geometry WKB")
}

func spatialDecodeWKB(data []byte, offset int) (spatialGeometry, bool) {
	if len(data) < offset+5 {
		return spatialGeometry{}, false
	}
	order := data[offset]
	if order != 0 && order != 1 {
		return spatialGeometry{}, false
	}
	read32 := func(at int) (uint32, bool) {
		if at < 0 || len(data) < at+4 {
			return 0, false
		}
		if order == 0 {
			return binary.BigEndian.Uint32(data[at : at+4]), true
		}
		return binary.LittleEndian.Uint32(data[at : at+4]), true
	}
	read64 := func(at int) (float64, bool) {
		if at < 0 || len(data) < at+8 {
			return 0, false
		}
		bits := binary.LittleEndian.Uint64(data[at : at+8])
		if order == 0 {
			bits = binary.BigEndian.Uint64(data[at : at+8])
		}
		return math.Float64frombits(bits), true
	}
	typeID, ok := read32(offset + 1)
	if !ok || typeID < 1 || typeID > 7 {
		return spatialGeometry{}, false
	}
	geometry := spatialGeometry{typeID: typeID}
	position := offset + 5
	readPoint := func() (spatialPoint, bool) {
		x, xOK := read64(position)
		y, yOK := read64(position + 8)
		if !xOK || !yOK {
			return spatialPoint{}, false
		}
		position += 16
		return spatialPoint{x: x, y: y}, true
	}
	switch typeID {
	case 1:
		point, pointOK := readPoint()
		if !pointOK {
			return spatialGeometry{}, false
		}
		if !math.IsNaN(point.x) || !math.IsNaN(point.y) {
			geometry.points = []spatialPoint{point}
		}
	case 2:
		count, countOK := read32(position)
		if !countOK || count > 1<<20 {
			return spatialGeometry{}, false
		}
		position += 4
		geometry.points = make([]spatialPoint, 0, count)
		for index := uint32(0); index < count; index++ {
			point, pointOK := readPoint()
			if !pointOK {
				return spatialGeometry{}, false
			}
			geometry.points = append(geometry.points, point)
		}
	case 3:
		ringCount, countOK := read32(position)
		if !countOK || ringCount > 1<<20 {
			return spatialGeometry{}, false
		}
		position += 4
		geometry.rings = make([][]spatialPoint, 0, ringCount)
		for ringIndex := uint32(0); ringIndex < ringCount; ringIndex++ {
			pointCount, pointCountOK := read32(position)
			if !pointCountOK || pointCount > 1<<20 {
				return spatialGeometry{}, false
			}
			position += 4
			ring := make([]spatialPoint, 0, pointCount)
			for pointIndex := uint32(0); pointIndex < pointCount; pointIndex++ {
				point, pointOK := readPoint()
				if !pointOK {
					return spatialGeometry{}, false
				}
				ring = append(ring, point)
			}
			geometry.rings = append(geometry.rings, ring)
		}
	case 4, 5, 6, 7:
		count, countOK := read32(position)
		if !countOK || count > 1<<20 {
			return spatialGeometry{}, false
		}
		position += 4
		geometry.children = make([]spatialGeometry, 0, count)
		for index := uint32(0); index < count; index++ {
			child, childOK := spatialDecodeWKB(data, position)
			if !childOK {
				return spatialGeometry{}, false
			}
			geometry.children = append(geometry.children, child)
			position += spatialWKBSize(child)
		}
	}
	return geometry, true
}

func spatialWKBSize(geometry spatialGeometry) int {
	size := 5
	switch geometry.typeID {
	case 1:
		size += 16
	case 2:
		size += 4 + 16*len(geometry.points)
	case 3:
		size += 4
		for _, ring := range geometry.rings {
			size += 4 + 16*len(ring)
		}
	case 4, 5, 6, 7:
		size += 4
		for _, child := range geometry.children {
			size += spatialWKBSize(child)
		}
	}
	return size
}

func spatialParseWKT(raw string) (spatialGeometry, error) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if strings.HasSuffix(upper, " EMPTY") || strings.HasSuffix(upper, " EMPTY") {
		kind := strings.Fields(upper)[0]
		geometryType, ok := spatialTypeID(kind)
		if !ok {
			return spatialGeometry{}, fmt.Errorf("unsupported geometry type %s", kind)
		}
		return spatialGeometry{typeID: geometryType}, nil
	}
	open := strings.IndexByte(upper, '(')
	if open < 0 || !strings.HasSuffix(text, ")") {
		return spatialGeometry{}, fmt.Errorf("invalid WKT geometry")
	}
	kind := strings.TrimSpace(upper[:open])
	geometryType, ok := spatialTypeID(kind)
	if !ok {
		return spatialGeometry{}, fmt.Errorf("unsupported geometry type %s", kind)
	}
	content := strings.TrimSpace(text[open+1 : len(text)-1])
	geometry := spatialGeometry{typeID: geometryType}
	parsePoint := func(rawPoint string) (spatialPoint, error) {
		parts := strings.Fields(strings.TrimSpace(rawPoint))
		if len(parts) < 2 {
			return spatialPoint{}, fmt.Errorf("invalid coordinate")
		}
		x, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			return spatialPoint{}, err
		}
		y, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return spatialPoint{}, err
		}
		return spatialPoint{x: x, y: y}, nil
	}
	switch geometryType {
	case 1:
		point, err := parsePoint(content)
		if err != nil {
			return spatialGeometry{}, err
		}
		geometry.points = []spatialPoint{point}
	case 2:
		points, err := spatialParsePointList(content, parsePoint)
		if err != nil {
			return spatialGeometry{}, err
		}
		geometry.points = points
	case 3:
		if len(content) < 4 || content[0] != '(' || content[len(content)-1] != ')' {
			return spatialGeometry{}, fmt.Errorf("invalid polygon WKT")
		}
		ringTexts := spatialSplitTopLevel(content)
		if len(ringTexts) == 1 && strings.HasPrefix(strings.TrimSpace(content), "((") {
			inner := strings.TrimSpace(content)[1 : len(strings.TrimSpace(content))-1]
			ringTexts = spatialSplitTopLevel(inner)
		}
		for _, ringText := range ringTexts {
			if len(ringText) < 2 {
				return spatialGeometry{}, fmt.Errorf("invalid polygon ring")
			}
			ring, err := spatialParsePointList(strings.Trim(ringText, "() "), parsePoint)
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.rings = append(geometry.rings, ring)
		}
	case 4:
		for _, pointText := range spatialSplitTopLevel(strings.TrimSpace(content)) {
			point, err := parsePoint(strings.Trim(pointText, "() "))
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, spatialGeometry{typeID: 1, points: []spatialPoint{point}})
		}
	case 5:
		inner := strings.TrimSpace(content)
		if len(inner) < 2 || inner[0] != '(' || inner[len(inner)-1] != ')' {
			return spatialGeometry{}, fmt.Errorf("invalid multilinestring WKT")
		}
		for _, lineText := range spatialSplitTopLevel(inner) {
			line, err := spatialParsePointList(strings.Trim(lineText, "() "), parsePoint)
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, spatialGeometry{typeID: 2, points: line})
		}
	case 6:
		inner := strings.TrimSpace(content)
		if len(inner) < 2 || inner[0] != '(' || inner[len(inner)-1] != ')' {
			return spatialGeometry{}, fmt.Errorf("invalid multipolygon WKT")
		}
		for _, polygonText := range spatialSplitTopLevel(inner) {
			polygon, err := spatialParseWKT("POLYGON" + strings.TrimSpace(polygonText))
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, polygon)
		}
	case 7:
		for _, childText := range spatialSplitTopLevel(content) {
			child, err := spatialParseWKT(strings.TrimSpace(childText))
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, child)
		}
	default:
		return spatialGeometry{}, fmt.Errorf("WKT geometry type %s is not supported by the compatibility evaluator", kind)
	}
	return geometry, nil
}

func spatialParseGeoJSON(raw string) (spatialGeometry, error) {
	var document struct {
		Type        string            `json:"type"`
		Coordinates json.RawMessage   `json:"coordinates"`
		Geometries  []json.RawMessage `json:"geometries"`
	}
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return spatialGeometry{}, fmt.Errorf("invalid GeoJSON: %w", err)
	}
	typeID, ok := spatialTypeID(strings.ToUpper(strings.TrimSpace(document.Type)))
	if !ok {
		return spatialGeometry{}, fmt.Errorf("unsupported GeoJSON geometry type %q", document.Type)
	}
	geometry := spatialGeometry{typeID: typeID}
	if typeID == 7 {
		for _, childRaw := range document.Geometries {
			child, err := spatialParseGeoJSON(string(childRaw))
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, child)
		}
		return geometry, nil
	}
	if len(document.Coordinates) == 0 || string(document.Coordinates) == "null" {
		return geometry, nil
	}
	var coordinates interface{}
	if err := json.Unmarshal(document.Coordinates, &coordinates); err != nil {
		return spatialGeometry{}, fmt.Errorf("invalid GeoJSON coordinates: %w", err)
	}
	pointFromValue := func(value interface{}) (spatialPoint, error) {
		parts, ok := value.([]interface{})
		if !ok || len(parts) < 2 {
			return spatialPoint{}, fmt.Errorf("GeoJSON coordinate must contain x and y")
		}
		x, xOK := parts[0].(float64)
		y, yOK := parts[1].(float64)
		if !xOK || !yOK {
			return spatialPoint{}, fmt.Errorf("GeoJSON coordinates must be numeric")
		}
		return spatialPoint{x: x, y: y}, nil
	}
	pointListFromValue := func(value interface{}) ([]spatialPoint, error) {
		parts, ok := value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("GeoJSON coordinate list is invalid")
		}
		points := make([]spatialPoint, 0, len(parts))
		for _, part := range parts {
			point, err := pointFromValue(part)
			if err != nil {
				return nil, err
			}
			points = append(points, point)
		}
		return points, nil
	}
	ringsFromValue := func(value interface{}) ([][]spatialPoint, error) {
		parts, ok := value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("GeoJSON polygon coordinates are invalid")
		}
		rings := make([][]spatialPoint, 0, len(parts))
		for _, part := range parts {
			ring, err := pointListFromValue(part)
			if err != nil {
				return nil, err
			}
			rings = append(rings, ring)
		}
		return rings, nil
	}
	switch typeID {
	case 1:
		point, err := pointFromValue(coordinates)
		if err != nil {
			return spatialGeometry{}, err
		}
		geometry.points = []spatialPoint{point}
	case 2:
		points, err := pointListFromValue(coordinates)
		if err != nil {
			return spatialGeometry{}, err
		}
		geometry.points = points
	case 3:
		rings, err := ringsFromValue(coordinates)
		if err != nil {
			return spatialGeometry{}, err
		}
		geometry.rings = rings
	case 4:
		parts, ok := coordinates.([]interface{})
		if !ok {
			return spatialGeometry{}, fmt.Errorf("GeoJSON multipoint coordinates are invalid")
		}
		for _, part := range parts {
			point, err := pointFromValue(part)
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, spatialGeometry{typeID: 1, points: []spatialPoint{point}})
		}
	case 5:
		parts, ok := coordinates.([]interface{})
		if !ok {
			return spatialGeometry{}, fmt.Errorf("GeoJSON multilinestring coordinates are invalid")
		}
		for _, part := range parts {
			points, err := pointListFromValue(part)
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, spatialGeometry{typeID: 2, points: points})
		}
	case 6:
		parts, ok := coordinates.([]interface{})
		if !ok {
			return spatialGeometry{}, fmt.Errorf("GeoJSON multipolygon coordinates are invalid")
		}
		for _, part := range parts {
			rings, err := ringsFromValue(part)
			if err != nil {
				return spatialGeometry{}, err
			}
			geometry.children = append(geometry.children, spatialGeometry{typeID: 3, rings: rings})
		}
	}
	return geometry, nil
}

func spatialGeoJSON(geometry spatialGeometry) (string, error) {
	point := func(value spatialPoint) []float64 { return []float64{value.x, value.y} }
	points := func(values []spatialPoint) [][]float64 {
		result := make([][]float64, len(values))
		for index, value := range values {
			result[index] = point(value)
		}
		return result
	}
	var document map[string]interface{}
	switch geometry.typeID {
	case 1:
		document = map[string]interface{}{"type": "Point"}
		if len(geometry.points) > 0 {
			document["coordinates"] = point(geometry.points[0])
		} else {
			document["coordinates"] = []float64{}
		}
	case 2:
		document = map[string]interface{}{"type": "LineString", "coordinates": points(geometry.points)}
	case 3:
		rings := make([][][]float64, len(geometry.rings))
		for index, ring := range geometry.rings {
			rings[index] = points(ring)
		}
		document = map[string]interface{}{"type": "Polygon", "coordinates": rings}
	case 4, 5, 6:
		children := make([]interface{}, len(geometry.children))
		for index, child := range geometry.children {
			childJSON, err := spatialGeoJSON(child)
			if err != nil {
				return "", err
			}
			var childDocument map[string]interface{}
			if err := json.Unmarshal([]byte(childJSON), &childDocument); err != nil {
				return "", err
			}
			children[index] = childDocument["coordinates"]
		}
		name := map[uint32]string{4: "MultiPoint", 5: "MultiLineString", 6: "MultiPolygon"}[geometry.typeID]
		document = map[string]interface{}{"type": name, "coordinates": children}
	case 7:
		children := make([]json.RawMessage, len(geometry.children))
		for index, child := range geometry.children {
			childJSON, err := spatialGeoJSON(child)
			if err != nil {
				return "", err
			}
			children[index] = json.RawMessage(childJSON)
		}
		return string(mustMarshalSpatialJSON(map[string]interface{}{"type": "GeometryCollection", "geometries": children})), nil
	default:
		return "", fmt.Errorf("unsupported spatial geometry type %d", geometry.typeID)
	}
	return string(mustMarshalSpatialJSON(document)), nil
}

func mustMarshalSpatialJSON(value interface{}) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		return []byte("null")
	}
	return raw
}

func spatialParsePointList(raw string, parsePoint func(string) (spatialPoint, error)) ([]spatialPoint, error) {
	parts := strings.Split(raw, ",")
	points := make([]spatialPoint, 0, len(parts))
	for _, part := range parts {
		point, err := parsePoint(part)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, nil
}

func spatialSplitTopLevel(raw string) []string {
	parts := make([]string, 0, 2)
	start, depth := 0, 0
	for index, character := range raw {
		switch character {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(raw[start:index]))
				start = index + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
	return parts
}

func spatialTypeID(name string) (uint32, bool) {
	switch strings.TrimSpace(name) {
	case "POINT":
		return 1, true
	case "LINESTRING":
		return 2, true
	case "POLYGON":
		return 3, true
	case "MULTIPOINT":
		return 4, true
	case "MULTILINESTRING":
		return 5, true
	case "MULTIPOLYGON":
		return 6, true
	case "GEOMETRYCOLLECTION":
		return 7, true
	default:
		return 0, false
	}
}

func spatialTypeName(typeID uint32) string {
	names := map[uint32]string{1: "ST_Point", 2: "ST_LineString", 3: "ST_Polygon", 4: "ST_MultiPoint", 5: "ST_MultiLineString", 6: "ST_MultiPolygon", 7: "ST_GeometryCollection"}
	if name, ok := names[typeID]; ok {
		return name
	}
	return "ST_Geometry"
}

func spatialWKT(geometry spatialGeometry) string {
	if spatialGeometryEmpty(geometry) {
		return strings.ToUpper(spatialTypeName(geometry.typeID)[3:]) + " EMPTY"
	}
	pointText := func(point spatialPoint) string {
		return strconv.FormatFloat(point.x, 'f', -1, 64) + " " + strconv.FormatFloat(point.y, 'f', -1, 64)
	}
	pointsText := func(points []spatialPoint) string {
		parts := make([]string, len(points))
		for index, point := range points {
			parts[index] = pointText(point)
		}
		return strings.Join(parts, ",")
	}
	switch geometry.typeID {
	case 1:
		return "POINT(" + pointText(geometry.points[0]) + ")"
	case 2:
		return "LINESTRING(" + pointsText(geometry.points) + ")"
	case 3:
		ringsText := make([]string, len(geometry.rings))
		for index, ring := range geometry.rings {
			ringsText[index] = "(" + pointsText(ring) + ")"
		}
		return "POLYGON(" + strings.Join(ringsText, ",") + ")"
	case 4:
		parts := make([]string, len(geometry.children))
		for index, child := range geometry.children {
			parts[index] = spatialWKT(child)[5:]
		}
		return "MULTIPOINT(" + strings.Join(parts, ",") + ")"
	case 5:
		parts := make([]string, len(geometry.children))
		for index, child := range geometry.children {
			parts[index] = spatialWKT(child)[10:]
		}
		return "MULTILINESTRING(" + strings.Join(parts, ",") + ")"
	case 6:
		parts := make([]string, len(geometry.children))
		for index, child := range geometry.children {
			parts[index] = spatialWKT(child)[7:]
		}
		return "MULTIPOLYGON(" + strings.Join(parts, ",") + ")"
	case 7:
		parts := make([]string, len(geometry.children))
		for index, child := range geometry.children {
			parts[index] = spatialWKT(child)
		}
		return "GEOMETRYCOLLECTION(" + strings.Join(parts, ",") + ")"
	default:
		return spatialTypeName(geometry.typeID) + " EMPTY"
	}
}

func spatialGeometryEmpty(geometry spatialGeometry) bool {
	return len(geometry.points) == 0 && len(geometry.rings) == 0 && len(geometry.children) == 0
}

func spatialGeometryValid(geometry spatialGeometry) bool {
	switch geometry.typeID {
	case 1:
		return len(geometry.points) == 1
	case 2:
		return len(geometry.points) >= 2
	case 3:
		if len(geometry.rings) == 0 {
			return false
		}
		for _, ring := range geometry.rings {
			if len(ring) < 4 || ring[0] != ring[len(ring)-1] || !spatialRingSimple(ring) || math.Abs(spatialRingSignedArea(ring)) == 0 {
				return false
			}
		}
		for index, hole := range geometry.rings[1:] {
			if !spatialRingContainsPoint(geometry.rings[0], hole[0]) {
				return false
			}
			for _, other := range geometry.rings[:index+1] {
				if spatialRingsIntersect(hole, other) {
					return false
				}
			}
		}
		return true
	case 4, 5, 6:
		if len(geometry.children) == 0 {
			return false
		}
		for _, child := range geometry.children {
			if !spatialGeometryValid(child) {
				return false
			}
		}
		return true
	case 7:
		for _, child := range geometry.children {
			if !spatialGeometryValid(child) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func spatialRingsIntersect(left, right []spatialPoint) bool {
	for leftIndex := 0; leftIndex < len(left)-1; leftIndex++ {
		for rightIndex := 0; rightIndex < len(right)-1; rightIndex++ {
			if spatialSegmentsIntersect(left[leftIndex], left[leftIndex+1], right[rightIndex], right[rightIndex+1]) {
				return true
			}
		}
	}
	return false
}

func spatialGeometrySimple(geometry spatialGeometry) bool {
	switch geometry.typeID {
	case 1:
		return len(geometry.points) <= 1
	case 2:
		return spatialLineSimple(geometry.points)
	case 3:
		if !spatialGeometryValid(geometry) {
			return false
		}
		for _, ring := range geometry.rings {
			if !spatialRingSimple(ring) {
				return false
			}
		}
		return true
	case 4, 5, 6, 7:
		for _, child := range geometry.children {
			if !spatialGeometrySimple(child) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func spatialLineSimple(points []spatialPoint) bool {
	if len(points) < 2 {
		return true
	}
	for index := 0; index < len(points)-1; index++ {
		if points[index] == points[index+1] {
			return false
		}
	}
	for left := 0; left < len(points)-1; left++ {
		for right := left + 1; right < len(points)-1; right++ {
			// Consecutive segments are allowed to meet at their shared vertex.
			if right == left+1 || (left == 0 && right == len(points)-2 && points[0] == points[len(points)-1]) {
				continue
			}
			if spatialSegmentsIntersect(points[left], points[left+1], points[right], points[right+1]) {
				return false
			}
		}
	}
	return true
}

func spatialRingSimple(ring []spatialPoint) bool {
	if len(ring) < 4 || ring[0] != ring[len(ring)-1] {
		return false
	}
	return spatialLineSimple(ring)
}

func spatialSegmentsIntersect(a, b, c, d spatialPoint) bool {
	const epsilon = 1e-12
	orientation := func(p, q, r spatialPoint) float64 {
		return (q.x-p.x)*(r.y-p.y) - (q.y-p.y)*(r.x-p.x)
	}
	onSegment := func(p, q, r spatialPoint) bool {
		return q.x >= math.Min(p.x, r.x)-epsilon && q.x <= math.Max(p.x, r.x)+epsilon &&
			q.y >= math.Min(p.y, r.y)-epsilon && q.y <= math.Max(p.y, r.y)+epsilon
	}
	o1, o2 := orientation(a, b, c), orientation(a, b, d)
	o3, o4 := orientation(c, d, a), orientation(c, d, b)
	if math.Abs(o1) <= epsilon && onSegment(a, c, b) || math.Abs(o2) <= epsilon && onSegment(a, d, b) ||
		math.Abs(o3) <= epsilon && onSegment(c, a, d) || math.Abs(o4) <= epsilon && onSegment(c, b, d) {
		return true
	}
	return (o1 > 0) != (o2 > 0) && (o3 > 0) != (o4 > 0)
}

func spatialPointOnSurface(geometry spatialGeometry) (spatialPoint, bool) {
	switch geometry.typeID {
	case 1:
		if len(geometry.points) == 1 {
			return geometry.points[0], true
		}
	case 2:
		if len(geometry.points) > 0 {
			return geometry.points[0], true
		}
	case 3:
		if len(geometry.rings) == 0 || len(geometry.rings[0]) == 0 {
			return spatialPoint{}, false
		}
		if centroid, ok := spatialPolygonCentroid(geometry.rings); ok && spatialPolygonContainsPoint(geometry, centroid) {
			return centroid, true
		}
		minX, minY, maxX, maxY, ok := spatialGeometryBounds(geometry)
		if ok {
			candidate := spatialPoint{x: (minX + maxX) / 2, y: (minY + maxY) / 2}
			if spatialPolygonContainsPoint(geometry, candidate) {
				return candidate, true
			}
		}
		return geometry.rings[0][0], true
	case 4, 5, 6, 7:
		for _, child := range geometry.children {
			if point, ok := spatialPointOnSurface(child); ok {
				return point, true
			}
		}
	}
	return spatialPoint{}, false
}

func spatialPredicate(name string, left, right spatialGeometry) (bool, error) {
	if left.srid != right.srid {
		return false, fmt.Errorf("spatial reference systems do not match")
	}
	if strings.HasPrefix(name, "MBR") {
		leftMinX, leftMinY, leftMaxX, leftMaxY, leftOK := spatialGeometryBounds(left)
		rightMinX, rightMinY, rightMaxX, rightMaxY, rightOK := spatialGeometryBounds(right)
		if !leftOK || !rightOK {
			return false, nil
		}
		switch name {
		case "MBRCONTAINS":
			return leftMinX <= rightMinX && leftMinY <= rightMinY && leftMaxX >= rightMaxX && leftMaxY >= rightMaxY, nil
		case "MBRWITHIN":
			return leftMinX >= rightMinX && leftMinY >= rightMinY && leftMaxX <= rightMaxX && leftMaxY <= rightMaxY, nil
		case "MBRINTERSECTS":
			return leftMinX <= rightMaxX && leftMaxX >= rightMinX && leftMinY <= rightMaxY && leftMaxY >= rightMinY, nil
		case "MBREQUALS":
			return leftMinX == rightMinX && leftMinY == rightMinY && leftMaxX == rightMaxX && leftMaxY == rightMaxY, nil
		case "MBRDISJOINT":
			return leftMaxX < rightMinX || rightMaxX < leftMinX || leftMaxY < rightMinY || rightMaxY < leftMinY, nil
		}
	}
	if name == "ST_EQUALS" {
		return spatialGeometryEqual(left, right), nil
	}
	if left.typeID == 1 && right.typeID == 1 {
		equal := left.points[0] == right.points[0]
		if name == "ST_INTERSECTS" {
			return equal, nil
		}
		return equal, nil
	}
	if name == "ST_WITHIN" {
		left, right = right, left
		name = "ST_CONTAINS"
	}
	if name == "ST_CONTAINS" {
		return spatialGeometryContains(left, right), nil
	}
	if name == "ST_INTERSECTS" {
		return spatialGeometryIntersects(left, right), nil
	}
	if name == "ST_DISJOINT" {
		return !spatialGeometryIntersects(left, right), nil
	}
	if name == "ST_TOUCHES" {
		return spatialGeometryTouches(left, right), nil
	}
	if name == "ST_OVERLAPS" {
		return spatialGeometryOverlaps(left, right), nil
	}
	if name == "ST_CROSSES" {
		return spatialGeometryCrosses(left, right), nil
	}
	return false, nil
}

func spatialGeometryBounds(geometry spatialGeometry) (float64, float64, float64, float64, bool) {
	points := make([]spatialPoint, 0)
	spatialCollectPoints(geometry, &points)
	if len(points) == 0 {
		return 0, 0, 0, 0, false
	}
	minX, minY := points[0].x, points[0].y
	maxX, maxY := minX, minY
	for _, point := range points[1:] {
		minX, minY = math.Min(minX, point.x), math.Min(minY, point.y)
		maxX, maxY = math.Max(maxX, point.x), math.Max(maxY, point.y)
	}
	return minX, minY, maxX, maxY, true
}

func spatialGeometryEqual(left, right spatialGeometry) bool {
	if left.typeID != right.typeID || left.srid != right.srid || len(left.points) != len(right.points) || len(left.rings) != len(right.rings) || len(left.children) != len(right.children) {
		return false
	}
	for index := range left.points {
		if left.points[index] != right.points[index] {
			return false
		}
	}
	for index := range left.rings {
		if len(left.rings[index]) != len(right.rings[index]) {
			return false
		}
		for pointIndex := range left.rings[index] {
			if left.rings[index][pointIndex] != right.rings[index][pointIndex] {
				return false
			}
		}
	}
	for index := range left.children {
		if !spatialGeometryEqual(left.children[index], right.children[index]) {
			return false
		}
	}
	return true
}

func spatialGeometryContains(container, candidate spatialGeometry) bool {
	if container.typeID >= 4 && container.typeID <= 7 {
		if candidate.typeID >= 4 && candidate.typeID <= 7 {
			for _, child := range candidate.children {
				if !spatialGeometryContains(container, child) {
					return false
				}
			}
			return true
		}
		for _, child := range container.children {
			if spatialGeometryContains(child, candidate) {
				return true
			}
		}
		return false
	}
	switch container.typeID {
	case 1:
		return candidate.typeID == 1 && len(container.points) == 1 && len(candidate.points) == 1 && container.points[0] == candidate.points[0]
	case 2:
		if candidate.typeID == 1 && len(candidate.points) == 1 {
			return spatialPointOnLine(container.points, candidate.points[0])
		}
		if candidate.typeID == 2 {
			for _, point := range candidate.points {
				if !spatialPointOnLine(container.points, point) {
					return false
				}
			}
			return len(candidate.points) > 0
		}
	case 3:
		points := make([]spatialPoint, 0)
		spatialCollectPoints(candidate, &points)
		if len(points) == 0 {
			return false
		}
		for _, point := range points {
			if !spatialPolygonContainsPoint(container, point) {
				return false
			}
		}
		return true
	}
	return false
}

func spatialGeometryIntersects(left, right spatialGeometry) bool {
	if left.typeID >= 4 && left.typeID <= 7 {
		for _, child := range left.children {
			if spatialGeometryIntersects(child, right) {
				return true
			}
		}
		return false
	}
	if right.typeID >= 4 && right.typeID <= 7 {
		for _, child := range right.children {
			if spatialGeometryIntersects(left, child) {
				return true
			}
		}
		return false
	}
	if left.typeID == 1 && len(left.points) == 1 {
		return spatialGeometryContainsPointOrLine(right, left.points[0])
	}
	if right.typeID == 1 && len(right.points) == 1 {
		return spatialGeometryContainsPointOrLine(left, right.points[0])
	}
	leftSegments := spatialGeometrySegments(left)
	rightSegments := spatialGeometrySegments(right)
	for _, leftSegment := range leftSegments {
		for _, rightSegment := range rightSegments {
			if spatialSegmentsIntersect(leftSegment[0], leftSegment[1], rightSegment[0], rightSegment[1]) {
				return true
			}
		}
	}
	leftPoints := make([]spatialPoint, 0)
	spatialCollectPoints(left, &leftPoints)
	for _, point := range leftPoints {
		if spatialGeometryContainsPointOrLine(right, point) {
			return true
		}
	}
	rightPoints := make([]spatialPoint, 0)
	spatialCollectPoints(right, &rightPoints)
	for _, point := range rightPoints {
		if spatialGeometryContainsPointOrLine(left, point) {
			return true
		}
	}
	return false
}

func spatialGeometryContainsPointOrLine(geometry spatialGeometry, point spatialPoint) bool {
	switch geometry.typeID {
	case 1:
		return len(geometry.points) == 1 && geometry.points[0] == point
	case 2:
		return spatialPointOnLine(geometry.points, point)
	case 3:
		return spatialPolygonCoversPoint(geometry, point)
	default:
		for _, child := range geometry.children {
			if spatialGeometryContainsPointOrLine(child, point) {
				return true
			}
		}
		return false
	}
}

func spatialGeometryTouches(left, right spatialGeometry) bool {
	if !spatialGeometryIntersects(left, right) || spatialGeometryContains(left, right) || spatialGeometryContains(right, left) {
		return false
	}
	return spatialBoundaryIntersects(left, right)
}

func spatialGeometryOverlaps(left, right spatialGeometry) bool {
	if spatialDimension(left) != spatialDimension(right) || !spatialGeometryIntersects(left, right) {
		return false
	}
	return !spatialGeometryContains(left, right) && !spatialGeometryContains(right, left)
}

func spatialGeometryCrosses(left, right spatialGeometry) bool {
	if left.typeID >= 4 && left.typeID <= 7 {
		for _, child := range left.children {
			if spatialGeometryCrosses(child, right) {
				return true
			}
		}
		return false
	}
	if right.typeID >= 4 && right.typeID <= 7 {
		for _, child := range right.children {
			if spatialGeometryCrosses(left, child) {
				return true
			}
		}
		return false
	}
	if left.typeID == 2 && right.typeID == 2 {
		return spatialLinesCross(left.points, right.points)
	}
	leftDimension, rightDimension := spatialDimension(left), spatialDimension(right)
	if leftDimension == rightDimension || leftDimension == 0 || rightDimension == 0 {
		return false
	}
	if leftDimension > rightDimension {
		left, right = right, left
	}
	if left.typeID == 2 && right.typeID == 3 {
		return spatialLineCrossesPolygon(left.points, right)
	}
	return false
}

func spatialLinesCross(left, right []spatialPoint) bool {
	if len(left) < 2 || len(right) < 2 {
		return false
	}
	for leftIndex := 1; leftIndex < len(left); leftIndex++ {
		for rightIndex := 1; rightIndex < len(right); rightIndex++ {
			a, b := left[leftIndex-1], left[leftIndex]
			c, d := right[rightIndex-1], right[rightIndex]
			if spatialProperSegmentIntersection(a, b, c, d) {
				return true
			}
			// A polyline's interior includes vertices between its endpoints. This
			// handles a crossing at such a vertex while excluding endpoint-only
			// touches and collinear overlaps.
			for _, point := range []spatialPoint{a, b, c, d} {
				if !spatialPointOnSegment(a, point, b) || !spatialPointOnSegment(c, point, d) {
					continue
				}
				if spatialLineInteriorPoint(left, point) && spatialLineInteriorPoint(right, point) {
					return true
				}
			}
		}
	}
	return false
}

func spatialProperSegmentIntersection(a, b, c, d spatialPoint) bool {
	const epsilon = 1e-12
	orientation := func(p, q, r spatialPoint) float64 {
		return (q.x-p.x)*(r.y-p.y) - (q.y-p.y)*(r.x-p.x)
	}
	o1, o2 := orientation(a, b, c), orientation(a, b, d)
	o3, o4 := orientation(c, d, a), orientation(c, d, b)
	return math.Abs(o1) > epsilon && math.Abs(o2) > epsilon && math.Abs(o3) > epsilon && math.Abs(o4) > epsilon &&
		(o1 > 0) != (o2 > 0) && (o3 > 0) != (o4 > 0)
}

func spatialLineInteriorPoint(points []spatialPoint, point spatialPoint) bool {
	if len(points) < 2 || point == points[0] || point == points[len(points)-1] {
		return false
	}
	return spatialPointOnLine(points, point)
}

func spatialLineCrossesPolygon(line []spatialPoint, polygon spatialGeometry) bool {
	if len(line) < 2 || len(polygon.rings) == 0 {
		return false
	}
	inside, outside := false, false
	for index := 1; index < len(line); index++ {
		start, end := line[index-1], line[index]
		for _, fraction := range []float64{0.01, 0.25, 0.5, 0.75, 0.99} {
			point := spatialPoint{x: start.x + (end.x-start.x)*fraction, y: start.y + (end.y-start.y)*fraction}
			if spatialPolygonContainsPoint(polygon, point) {
				inside = true
			} else if !spatialPolygonCoversPoint(polygon, point) {
				outside = true
			}
			if inside && outside {
				return true
			}
		}
	}
	return inside && outside
}

func spatialBoundaryIntersects(left, right spatialGeometry) bool {
	leftSegments := spatialGeometrySegments(left)
	rightSegments := spatialGeometrySegments(right)
	for _, leftSegment := range leftSegments {
		for _, rightSegment := range rightSegments {
			if spatialSegmentsIntersect(leftSegment[0], leftSegment[1], rightSegment[0], rightSegment[1]) {
				return true
			}
		}
	}
	leftPoints := spatialBoundaryPoints(left)
	rightPoints := spatialBoundaryPoints(right)
	for _, leftPoint := range leftPoints {
		for _, rightPoint := range rightPoints {
			if leftPoint == rightPoint {
				return true
			}
		}
	}
	return false
}

func spatialBoundaryPoints(geometry spatialGeometry) []spatialPoint {
	switch geometry.typeID {
	case 1:
		return append([]spatialPoint(nil), geometry.points...)
	case 2:
		if len(geometry.points) < 2 {
			return append([]spatialPoint(nil), geometry.points...)
		}
		return []spatialPoint{geometry.points[0], geometry.points[len(geometry.points)-1]}
	case 3:
		points := make([]spatialPoint, 0)
		for _, ring := range geometry.rings {
			points = append(points, ring...)
		}
		return points
	default:
		points := make([]spatialPoint, 0)
		for _, child := range geometry.children {
			points = append(points, spatialBoundaryPoints(child)...)
		}
		return points
	}
}

func spatialGeometrySegments(geometry spatialGeometry) [][2]spatialPoint {
	segments := make([][2]spatialPoint, 0)
	appendPoints := func(points []spatialPoint, closed bool) {
		for index := 1; index < len(points); index++ {
			segments = append(segments, [2]spatialPoint{points[index-1], points[index]})
		}
		if closed && len(points) > 2 && points[0] != points[len(points)-1] {
			segments = append(segments, [2]spatialPoint{points[len(points)-1], points[0]})
		}
	}
	switch geometry.typeID {
	case 2:
		appendPoints(geometry.points, false)
	case 3:
		for _, ring := range geometry.rings {
			appendPoints(ring, true)
		}
	default:
		for _, child := range geometry.children {
			segments = append(segments, spatialGeometrySegments(child)...)
		}
	}
	return segments
}

func spatialPointOnLine(points []spatialPoint, point spatialPoint) bool {
	for index := 1; index < len(points); index++ {
		if spatialPointOnSegment(points[index-1], point, points[index]) {
			return true
		}
	}
	return false
}

func spatialPointOnSegment(start, point, end spatialPoint) bool {
	const epsilon = 1e-12
	cross := (point.x-start.x)*(end.y-start.y) - (point.y-start.y)*(end.x-start.x)
	if math.Abs(cross) > epsilon {
		return false
	}
	return point.x >= math.Min(start.x, end.x)-epsilon && point.x <= math.Max(start.x, end.x)+epsilon &&
		point.y >= math.Min(start.y, end.y)-epsilon && point.y <= math.Max(start.y, end.y)+epsilon
}

func spatialGeometryDistance(left, right spatialGeometry) (float64, bool) {
	if spatialGeometryIntersects(left, right) {
		return 0, true
	}
	leftPoints := make([]spatialPoint, 0)
	rightPoints := make([]spatialPoint, 0)
	spatialCollectPoints(left, &leftPoints)
	spatialCollectPoints(right, &rightPoints)
	leftSegments := spatialGeometrySegments(left)
	rightSegments := spatialGeometrySegments(right)
	if len(leftPoints) == 0 || len(rightPoints) == 0 {
		return 0, false
	}
	minimum := math.Inf(1)
	for _, leftPoint := range leftPoints {
		for _, rightPoint := range rightPoints {
			minimum = math.Min(minimum, math.Hypot(leftPoint.x-rightPoint.x, leftPoint.y-rightPoint.y))
		}
		for _, segment := range rightSegments {
			minimum = math.Min(minimum, spatialPointSegmentDistance(leftPoint, segment[0], segment[1]))
		}
	}
	for _, rightPoint := range rightPoints {
		for _, segment := range leftSegments {
			minimum = math.Min(minimum, spatialPointSegmentDistance(rightPoint, segment[0], segment[1]))
		}
	}
	for _, leftSegment := range leftSegments {
		for _, rightSegment := range rightSegments {
			minimum = math.Min(minimum, spatialPointSegmentDistance(leftSegment[0], rightSegment[0], rightSegment[1]))
			minimum = math.Min(minimum, spatialPointSegmentDistance(leftSegment[1], rightSegment[0], rightSegment[1]))
			minimum = math.Min(minimum, spatialPointSegmentDistance(rightSegment[0], leftSegment[0], leftSegment[1]))
			minimum = math.Min(minimum, spatialPointSegmentDistance(rightSegment[1], leftSegment[0], leftSegment[1]))
		}
	}
	return minimum, !math.IsInf(minimum, 1)
}

func spatialPointSegmentDistance(point, start, end spatialPoint) float64 {
	dx, dy := end.x-start.x, end.y-start.y
	lengthSquared := dx*dx + dy*dy
	if lengthSquared == 0 {
		return math.Hypot(point.x-start.x, point.y-start.y)
	}
	t := ((point.x-start.x)*dx + (point.y-start.y)*dy) / lengthSquared
	if t < 0 {
		t = 0
	} else if t > 1 {
		t = 1
	}
	closest := spatialPoint{x: start.x + t*dx, y: start.y + t*dy}
	return math.Hypot(point.x-closest.x, point.y-closest.y)
}

func spatialPolygonContainsPoint(polygon spatialGeometry, point spatialPoint) bool {
	if len(polygon.rings) == 0 {
		return false
	}
	inside := spatialRingContainsPoint(polygon.rings[0], point)
	for _, hole := range polygon.rings[1:] {
		if spatialRingContainsPoint(hole, point) {
			return false
		}
	}
	return inside
}

func spatialPolygonCoversPoint(polygon spatialGeometry, point spatialPoint) bool {
	if spatialPolygonContainsPoint(polygon, point) {
		return true
	}
	for _, ring := range polygon.rings {
		if spatialPointOnLine(ring, point) {
			return true
		}
	}
	return false
}

func spatialRingContainsPoint(ring []spatialPoint, point spatialPoint) bool {
	inside := false
	for index, previous := 0, len(ring)-1; index < len(ring); previous, index = index, index+1 {
		current := ring[index]
		previousPoint := ring[previous]
		if (current.y > point.y) != (previousPoint.y > point.y) && point.x < (previousPoint.x-current.x)*(point.y-current.y)/(previousPoint.y-current.y)+current.x {
			inside = !inside
		}
	}
	return inside
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
