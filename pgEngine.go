package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type pgEngine struct {
	db         *sql.DB
	bucketName []byte
}

func newPgEngine(db *sql.DB) *pgEngine {
	return &pgEngine{db, []byte("data")}
}

func (pe *pgEngine) execute(tree *pgquery.ParseResult) error {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return pe.executeCreate(c)
		}

		if c := n.GetInsertStmt(); c != nil {
			return pe.executeInsert(c)
		}

		if c := n.GetDeleteStmt(); c != nil {
			return pe.executeDelete(c)
		}

		if c := n.GetSelectStmt(); c != nil {
			_, err := pe.executeSelect(c)
			return err
		}

		// return fmt.Errorf("unknown statement type: %s", stmt)
		return nil
	}

	return nil
}

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

func (pe *pgEngine) executeCreate(stmt *pgquery.CreateStmt) error {
	tbl := tableDefinition{}
	tbl.Name = stmt.Relation.Relname

	for _, c := range stmt.TableElts {
		cd := c.GetColumnDef()

		tbl.ColumnNames = append(tbl.ColumnNames, cd.Colname)

		// Names is namespaced. So `INT` is pg_catalog.int4. `BIGINT` is pg_catalog.int8.
		var columnType string
		for _, n := range cd.TypeName.Names {
			if columnType != "" {
				columnType += "."
			}
			columnType += n.GetString_().Str
		}
		tbl.ColumnTypes = append(tbl.ColumnTypes, columnType)
	}

	tableBytes, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("could not marshal table: %s", err)
	}

	statement, err := pe.db.Prepare(`INSERT INTO kv ("key", "bytes") VALUES (?, ?);`)
	if err != nil {
		log.Fatal(err.Error())
	}
	_, err = statement.Exec("tables_"+tbl.Name, tableBytes)

	if err != nil {
		return fmt.Errorf("could not set key-value: %s", err)
	}

	return nil
}

func (pe *pgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	var tbl tableDefinition

	statement, err := pe.db.Prepare(`SELECT bytes FROM kv WHERE "key" = ?;`)
	if err != nil {
		log.Fatal(err.Error())
	}
	var tableBytes []byte
	fmt.Println("tables_" + name)
	err = statement.QueryRow("tables_" + name).Scan(&tableBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get table bytes from SQLite: %s", err)
	}
	fmt.Println("tableBytes: ", tableBytes)
	err = json.Unmarshal(tableBytes, &tbl)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal table: %s", err)
	}
	fmt.Println("tbl: ", tbl)
	return &tbl, err
}

func (pe *pgEngine) executeInsert(stmt *pgquery.InsertStmt) error {
	tblName := stmt.Relation.Relname

	slct := stmt.GetSelectStmt().GetSelectStmt()
	for _, values := range slct.ValuesLists {
		var rowData []any
		for _, value := range values.GetList().Items {
			if c := value.GetAConst(); c != nil {
				if s := c.Val.GetString_(); s != nil {
					rowData = append(rowData, s.Str)
					continue
				}

				if i := c.Val.GetInteger(); i != nil {
					rowData = append(rowData, i.Ival)
					continue
				}
			}

			return fmt.Errorf("unknown value type: %s", value)
		}
		rowBytes, err := json.Marshal(rowData)
		if err != nil {
			return fmt.Errorf("could not marshal row: %s", err)
		}

		id := uuid.New().String()
		statement, err := pe.db.Prepare(`INSERT INTO kv ("key", "bytes") VALUES (?, ?);`)

		if err != nil {
			log.Fatal(err.Error())
		}
		_, err = statement.Exec("rows_"+tblName+"_"+id, rowBytes)

		if err != nil {
			return fmt.Errorf("could not store row: %s", err)
		}
	}

	return nil
}

func (pe *pgEngine) executeDelete(stmt *pgquery.DeleteStmt) error {
	// TODO: implement where, delete for now deletes everything excluding table defn
	_, err := pe.db.Exec("DELETE FROM kv where key like 'rows_%'")
	if err != nil {
		return fmt.Errorf("could not delete table: %s", err)
	}
	return nil
}

type pgResult struct {
	fieldNames []string
	fieldTypes []string
	rows       [][]any
}

func (pe *pgEngine) executeSelect(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	fmt.Println("tblName: ", tblName)
	tbl, err := pe.getTableDefinition(tblName)
	fmt.Println("tbl: ", tbl)
	if err != nil {
		return nil, err
	}

	results := &pgResult{}
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.fieldNames = append(results.fieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("unknown field: %s", fieldName)
		}

		results.fieldTypes = append(results.fieldTypes, fieldType)
	}
	prefix := "rows_" + tblName + "_"

	fmt.Println("prefix: ", prefix)
	statement, err := pe.db.Prepare(`SELECT bytes FROM kv WHERE key LIKE ? || '%';`)
	if err != nil {
		log.Fatal(err.Error())
	}

	rows, err := statement.Query(prefix)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var rowBytes []byte
		err = rows.Scan(&rowBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to get row bytes from SQLite: %s", err)
		}
		fmt.Println("rowBytes: ", rowBytes)
		// wrap!
		var row []any
		err = json.Unmarshal(rowBytes, &row)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal row: %s", err)
		}
		var targetRow []any
		for _, target := range results.fieldNames {
			for i, field := range tbl.ColumnNames {
				if target == field {
					targetRow = append(targetRow, row[i])
				}
			}
		}
		results.rows = append(results.rows, targetRow)
	}
	return results, nil
}

func (pe *pgEngine) delete() error {
	_, err := pe.db.Exec("DELETE FROM kv")
	if err != nil {
		return fmt.Errorf("could not delete table: %s", err)
	}

	return nil
}
