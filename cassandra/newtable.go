package cassandra

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
)

// NewTable creates a new table in database (if a table doesn't exist).
// This always returns table-structures as per provided definition,
// even if the table already existed in database (in which case
// the provided-definition is still not cross-checked/synced
// with the actual table from database).
func NewTable(
	session driver.SessionI,
	tc *TableConfig,
	definition *map[string]TableColumn,
) (*Table, error) {
	if definition == nil || len(*definition) == 0 {
		return nil, errors.New("Table Definition not set")
	}
	if tc.Name == "" {
		return nil, errors.New("Table name is required")
	}

	var name string
	if tc.Keyspace != "" {
		name = tc.Keyspace + "."
	}
	name += tc.Name
	schema, err := schemaFromDefinition(definition)

	t := &Table{
		definition: definition,
		keyspace:   tc.Keyspace,
		name:       tc.Name,
		initQueryx: driver.NewQueryx,
		initIterx:  driver.NewIterx,
		session:    session,
		schema:     schema,
	}

	tableColumns := ""
	clusteringOrder := ""
	if err != nil {
		return nil, err
	}
	for key, value := range *schema {
		if key != "WITH CLUSTERING ORDER BY" {
			tableColumns += fmt.Sprintf("%s %s, ", key, value)
		} else {
			clusteringOrder += fmt.Sprintf("%s %s", key, value)
		}
	}
	tableColumns = strings.TrimSuffix(tableColumns, ", ")

	query := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (%s) %s`,
		name,
		tableColumns,
		clusteringOrder,
	)

	err = session.Query(query).Exec()
	if err != nil {
		return nil, err
	}
	return t, nil
}

// schemaFromDefinition creates table-schema (can be used for creating table)
// from provided definition. The resulting map has column-names as keys and
// data-types as values, and also includes "PRIMARY KEY" and "CLUSTERING ORDER"
// keys/values.
func schemaFromDefinition(
	tableDefinition *map[string]TableColumn,
) (*map[string]string, error) {
	schema := make(map[string]string)
	// Sample layout:
	//  map[
	//   0:map[column:month_bucket]
	//   1:map[column:timestamp order:DESC]
	//   2:map[column:uuid order:ASC]
	//  ]
	primaryKeyDefinition := make(map[int]map[string]string)

	for _, columnDefinition := range *tableDefinition {
		columnName := columnDefinition.Name
		schema[columnName] = columnDefinition.DataType
		// Convert string key-index to integer, and build primary-key-schema
		primaryKeyIndexStr := columnDefinition.PrimaryKeyIndex
		primaryKeyOrder := columnDefinition.PrimaryKeyOrder
		var err error

		if primaryKeyOrder != "" && primaryKeyIndexStr == "" {
			return nil, errors.New(
				"PrimaryKeyOrder cannot be specified without specifying PrimaryKeyIndex." +
					fmt.Sprintf(" Errored Key: \"%s\"", columnName),
			)
		}

		if primaryKeyIndexStr != "" {
			primaryKeyIndex, _ := strconv.Atoi(primaryKeyIndexStr)
			// Sample primaryKeySchema:
			//  &map[column:uuid order:ASC]
			primaryKeySchema, schemaErr := buildPrimaryKeySchema(
				columnName,
				primaryKeyIndexStr,
				primaryKeyOrder,
			)

			if schemaErr == nil {
				if primaryKeyDefinition[primaryKeyIndex] != nil {
					previousPrimaryKey := primaryKeyDefinition[primaryKeyIndex]["column"]
					currKey := (*primaryKeySchema)["column"]
					return nil, errors.New(
						"Duplicate Primary Key Index found. This might result in a Primary Key" +
							" overriding another Primary Key and cause unexpected behavior." +
							fmt.Sprintf(" Previous key with same index: \"%s\". Current Key: \"%s\"", previousPrimaryKey, currKey),
					)
				}
				primaryKeyDefinition[primaryKeyIndex] = *primaryKeySchema
			}
			err = schemaErr
		}
		if err != nil {
			return nil, err
		}
	}
	primaryKeyStr, clusteringKeyOrderStr := primaryKeySchemaToQueryString(&primaryKeyDefinition)
	schema["PRIMARY KEY"] = fmt.Sprintf("(%s)", primaryKeyStr)
	schema["WITH CLUSTERING ORDER BY"] = fmt.Sprintf("(%s)", clusteringKeyOrderStr)

	return &schema, nil
}

// buildPrimaryKeySchema returns map with Primary-Key schema using
// the provided values. The primaryKeyIndexStr must have a valid value
// (an integer in string format, example: "1"), else a blank map is returned.
// Sample layout of the map returned:
//  &map[column:uuid order:ASC]
func buildPrimaryKeySchema(
	columnName string,
	primaryKeyIndexStr string,
	primaryKeyOrder string,
) (*map[string]string, error) {
	if primaryKeyOrder != "" && primaryKeyIndexStr == "0" {
		return nil, errors.New(
			"PrimaryKeyOrder cannot be specified if PrimaryKeyIndex is 0." +
				fmt.Sprintf(" Errored Key: \"%s\"", columnName),
		)
	}

	primaryKey := make(map[string]string)
	primaryKeyIndex, err := strconv.Atoi(primaryKeyIndexStr)
	if err != nil {
		return nil, err
	}
	primaryKey["column"] = columnName

	// Partitioning-key has no order
	if primaryKeyIndex > 0 {
		switch strings.ToUpper(primaryKeyOrder) {
		case "DESC":
			primaryKey["order"] = "DESC"
		case "ASC":
			primaryKey["order"] = "ASC"
		case "":
			primaryKey["order"] = "ASC"
		default:
			err := fmt.Errorf(
				"Invalid PrimaryKeyOrder specified: \"%s\". Valid values are: \"DESC\" or \"ASC\""+
					" Errored Key: \"%s\"",
				primaryKeyOrder,
				columnName,
			)
			return nil, err
		}
	}
	return &primaryKey, nil
}

// primaryKeySchemaToQueryString transforms primary-key-schema into strings
// that can be directly used for table-declaration (or creation) in database.
// Returns two strings: primary-key and clustering-key-order respectively.
func primaryKeySchemaToQueryString(
	primaryKeyDefinition *map[int]map[string]string,
) (string, string) {
	primaryKeys := make([][]string, len(*primaryKeyDefinition))
	for key, value := range *primaryKeyDefinition {
		primaryKeys[key] = []string{
			value["column"],
			value["order"],
		}
	}

	primaryKeyStr := ""
	clusteringKeyOrderStr := ""
	for index, value := range primaryKeys {
		primaryKeyStr += fmt.Sprintf("%s, ", value[0])
		if index > 0 {
			clusteringKeyOrderStr += fmt.Sprintf("%s %s, ", value[0], value[1])
		}
	}

	primaryKeyStr = strings.TrimSuffix(primaryKeyStr, ", ")
	clusteringKeyOrderStr = strings.TrimSuffix(clusteringKeyOrderStr, ", ")
	return primaryKeyStr, clusteringKeyOrderStr
}
