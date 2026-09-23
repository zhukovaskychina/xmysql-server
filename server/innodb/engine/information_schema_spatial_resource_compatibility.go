package engine

import (
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// MySQL ships a fixed set of linear units for ST_Distance.  Keep the catalog
// data in the same order and with the same factors as MySQL 8.4's GIS unit
// table.  Descriptions are empty in the upstream 8.4 unit definitions.
type informationSchemaUnit struct {
	name   string
	factor float64
}

var informationSchemaUnits = []informationSchemaUnit{
	{name: "millimetre", factor: 0.001},
	{name: "centimetre", factor: 0.01},
	{name: "metre", factor: 1},
	{name: "foot", factor: 0.3048},
	{name: "US survey foot", factor: 0.30480060960121924},
	{name: "Clarke's foot", factor: 0.3047972654},
	{name: "fathom", factor: 1.8288},
	{name: "nautical mile", factor: 1852},
	{name: "German legal metre", factor: 1.0000135965},
	{name: "US survey chain", factor: 20.11684023368047},
	{name: "US survey link", factor: 0.2011684023368047},
	{name: "US survey mile", factor: 1609.3472186944375},
	{name: "kilometre", factor: 1000},
	{name: "Clarke's yard", factor: 0.9143917962},
	{name: "Clarke's chain", factor: 20.1166195164},
	{name: "Clarke's link", factor: 0.201166195164},
	{name: "British yard (Sears 1922)", factor: 0.9143984146160287},
	{name: "British foot (Sears 1922)", factor: 0.3047994715386762},
	{name: "British chain (Sears 1922)", factor: 20.116765121552632},
	{name: "British link (Sears 1922)", factor: 0.2011676512155263},
	{name: "British yard (Benoit 1895 A)", factor: 0.9143992},
	{name: "British foot (Benoit 1895 A)", factor: 0.3047997333333333},
	{name: "British chain (Benoit 1895 A)", factor: 20.1167824},
	{name: "British link (Benoit 1895 A)", factor: 0.201167824},
	{name: "British yard (Benoit 1895 B)", factor: 0.9143992042898124},
	{name: "British foot (Benoit 1895 B)", factor: 0.30479973476327077},
	{name: "British chain (Benoit 1895 B)", factor: 20.116782494375872},
	{name: "British link (Benoit 1895 B)", factor: 0.2011678249437587},
	{name: "British foot (1865)", factor: 0.30480083333333335},
	{name: "Indian foot", factor: 0.30479951024814694},
	{name: "Indian foot (1937)", factor: 0.30479841},
	{name: "Indian foot (1962)", factor: 0.3047996},
	{name: "Indian foot (1975)", factor: 0.3047995},
	{name: "Indian yard", factor: 0.9143985307444408},
	{name: "Indian yard (1937)", factor: 0.91439523},
	{name: "Indian yard (1962)", factor: 0.9143988},
	{name: "Indian yard (1975)", factor: 0.9143985},
	{name: "Statute mile", factor: 1609.344},
	{name: "Gold Coast foot", factor: 0.3047997101815088},
	{name: "British foot (1936)", factor: 0.3048007491},
	{name: "yard", factor: 0.9144},
	{name: "chain", factor: 20.1168},
	{name: "link", factor: 0.201168},
	{name: "British yard (Sears 1922 truncated)", factor: 0.914398},
	{name: "British foot (Sears 1922 truncated)", factor: 0.30479933333333337},
	{name: "British chain (Sears 1922 truncated)", factor: 20.116756},
	{name: "British link (Sears 1922 truncated)", factor: 0.20116756},
}

type informationSchemaSRS struct {
	name           string
	id             uint64
	organization   interface{}
	organizationID interface{}
	definition     string
	description    interface{}
}

// SRID 0 is mandatory in every MySQL installation.  4326 and 3857 are the
// two most commonly consumed EPSG entries and are included as deterministic
// built-ins.  The complete MySQL EPSG catalog still requires an external
// catalog import and remains tracked separately as partial.
var informationSchemaBuiltInSRS = []informationSchemaSRS{
	{name: "", id: 0, organization: nil, organizationID: nil, definition: "", description: nil},
	{
		name: "WGS 84", id: 4326, organization: "EPSG", organizationID: uint64(4326),
		definition:  `GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]]`,
		description: nil,
	},
	{
		name: "WGS 84 / Pseudo-Mercator", id: 3857, organization: "EPSG", organizationID: uint64(3857),
		definition:  `PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Popular Visualisation Pseudo Mercator",AUTHORITY["EPSG","1024"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",0,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","3857"]]`,
		description: nil,
	},
}

var informationSchemaCatalogFilterPattern = regexp.MustCompile(`(?is)\b(unit_name|unit_type|conversion_factor|srs_id|srs_name|organization|organization_coordsys_id|definition|description|resource_group_name|resource_group_type|resource_group_enabled|vcpu_ids|thread_priority)\b\s*(=|like)\s*(?:'([^']*)'|"([^"]*)"|([-+]?[0-9]+(?:\.[0-9]+)?))`)

func informationSchemaCatalogFilter(query, column string) string {
	for _, match := range informationSchemaCatalogFilterPattern.FindAllStringSubmatch(query, -1) {
		if !strings.EqualFold(match[1], column) {
			continue
		}
		if match[3] != "" {
			return match[3]
		}
		if match[4] != "" {
			return match[4]
		}
		return match[5]
	}
	return ""
}

func (e *XMySQLExecutor) executeInformationSchemaSTUnitsOfMeasureSelect(query string) *SelectResult {
	const table = "st_units_of_measure"
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[table])
	nameFilter := informationSchemaCatalogFilter(query, "unit_name")
	typeFilter := informationSchemaCatalogFilter(query, "unit_type")
	factorFilter := informationSchemaCatalogFilter(query, "conversion_factor")
	rows := make([][]interface{}, 0, len(informationSchemaUnits))
	for _, unit := range informationSchemaUnits {
		factor := strconv.FormatFloat(unit.factor, 'g', -1, 64)
		if !metadataPatternMatches(unit.name, nameFilter) || !metadataPatternMatches("LINEAR", typeFilter) ||
			!metadataPatternMatches(factor, factorFilter) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"UNIT_NAME": unit.name, "UNIT_TYPE": "LINEAR", "CONVERSION_FACTOR": unit.factor, "DESCRIPTION": "",
		}))
	}
	return newInformationSchemaSelectResult("information_schema."+table, columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaSTSpatialReferenceSystemsSelect(query string) *SelectResult {
	const table = "st_spatial_reference_systems"
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[table])
	idFilter := informationSchemaCatalogFilter(query, "srs_id")
	nameFilter := informationSchemaCatalogFilter(query, "srs_name")
	organizationFilter := informationSchemaCatalogFilter(query, "organization")
	organizationIDFilter := informationSchemaCatalogFilter(query, "organization_coordsys_id")
	definitionFilter := informationSchemaCatalogFilter(query, "definition")
	descriptionFilter := informationSchemaCatalogFilter(query, "description")
	rows := make([][]interface{}, 0, len(informationSchemaBuiltInSRS))
	for _, srs := range informationSchemaBuiltInSRS {
		if idFilter != "" && strconv.FormatUint(srs.id, 10) != idFilter {
			continue
		}
		if nameFilter != "" && !metadataPatternMatches(srs.name, nameFilter) {
			continue
		}
		if organizationFilter != "" && !informationSchemaCatalogInterfaceMatches(srs.organization, organizationFilter) {
			continue
		}
		if organizationIDFilter != "" && !informationSchemaCatalogInterfaceMatches(srs.organizationID, organizationIDFilter) {
			continue
		}
		if definitionFilter != "" && !metadataPatternMatches(srs.definition, definitionFilter) {
			continue
		}
		if descriptionFilter != "" && !informationSchemaCatalogInterfaceMatches(srs.description, descriptionFilter) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"SRS_NAME": srs.name, "SRS_ID": srs.id, "ORGANIZATION": srs.organization,
			"ORGANIZATION_COORDSYS_ID": srs.organizationID, "DEFINITION": srs.definition, "DESCRIPTION": srs.description,
		}))
	}
	return newInformationSchemaSelectResult("information_schema."+table, columns, rows)
}

func informationSchemaCatalogInterfaceMatches(value interface{}, pattern string) bool {
	if value == nil {
		return false
	}
	return metadataPatternMatches(fmt.Sprint(value), pattern)
}

func (e *XMySQLExecutor) executeInformationSchemaResourceGroupsSelect(query string) *SelectResult {
	const table = "resource_groups"
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[table])
	nameFilter := informationSchemaCatalogFilter(query, "resource_group_name")
	typeFilter := informationSchemaCatalogFilter(query, "resource_group_type")
	enabledFilter := informationSchemaCatalogFilter(query, "resource_group_enabled")
	vcpuFilter := informationSchemaCatalogFilter(query, "vcpu_ids")
	priorityFilter := informationSchemaCatalogFilter(query, "thread_priority")
	vcpuIDs := ""
	if cpus := runtime.NumCPU(); cpus > 0 {
		vcpuIDs = fmt.Sprintf("0-%d", cpus-1)
	}
	groups := []struct {
		name, groupType string
	}{
		{name: "USR_default", groupType: "USER"},
		{name: "SYS_default", groupType: "SYSTEM"},
	}
	rows := make([][]interface{}, 0, len(groups))
	for _, group := range groups {
		if !metadataPatternMatches(group.name, nameFilter) || !metadataPatternMatches(group.groupType, typeFilter) ||
			!metadataPatternMatches("1", enabledFilter) || !metadataPatternMatches(vcpuIDs, vcpuFilter) ||
			!metadataPatternMatches("0", priorityFilter) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"RESOURCE_GROUP_NAME": group.name, "RESOURCE_GROUP_TYPE": group.groupType,
			"RESOURCE_GROUP_ENABLED": int64(1), "VCPU_IDS": vcpuIDs, "THREAD_PRIORITY": int64(0),
		}))
	}
	return newInformationSchemaSelectResult("information_schema."+table, columns, rows)
}
