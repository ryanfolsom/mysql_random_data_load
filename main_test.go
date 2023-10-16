package main

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ryanfolsom/mysql_random_data_load/internal/getters"
	"github.com/ryanfolsom/mysql_random_data_load/tableparser"
	tu "github.com/ryanfolsom/mysql_random_data_load/testutils"
	"github.com/stretchr/testify/assert"
)

func TestGetSamples(t *testing.T) {
	conn := tu.GetMySQLConnection(t)
	var wantRows int64 = 100
	samples, err := getSamples(conn, "sakila", "inventory", "inventory_id", wantRows, "int")
	tu.Ok(t, err, "error getting samples")
	_, ok := samples[0].(int64)
	tu.Assert(t, ok, "Wrong data type.")
	tu.Assert(t, int64(len(samples)) == wantRows,
		"Wrong number of samples. Have %d, want 100.", len(samples))
}

func TestGenerateInsertData(t *testing.T) {
	wantRows := 3

	values := []getter{
		getters.NewRandomInt("f1", 100, false),
		getters.NewRandomString("f2", 10, false),
		getters.NewRandomDate("f3", false),
	}

	rowsChan := make(chan []getter, 100)
	count := 0
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for {
			select {
			case <-time.After(10 * time.Millisecond):
				wg.Done()
				return
			case row := <-rowsChan:
				if reflect.TypeOf(row[0]).String() != "*getters.RandomInt" {
					fmt.Printf("Expected '*getters.RandomInt' for field [0], got %q\n", reflect.TypeOf(row[0]).String())
					t.Fail()
				}
				if reflect.TypeOf(row[1]).String() != "*getters.RandomString" {
					fmt.Printf("Expected '*getters.RandomString' for field [1], got %q\n", reflect.TypeOf(row[1]).String())
					t.Fail()
				}
				if reflect.TypeOf(row[2]).String() != "*getters.RandomDate" {
					fmt.Printf("Expected '*getters.RandomDate' for field [2], got %q\n", reflect.TypeOf(row[2]).String())
					t.Fail()
				}
				count++
			}
		}
	}()

	generateInsertData(wantRows, values, rowsChan)

	wg.Wait()
	tu.Assert(t, count == 3, "Invalid number of rows")
}

func TestGenerateInsertStmt(t *testing.T) {
	var table *tableparser.Table
	tu.LoadJson(t, "sakila.film.json", &table)
	want := "INSERT IGNORE INTO `sakila`.`film` " +
		"(`title`,`description`,`release_year`,`language_id`," +
		"`original_language_id`,`rental_duration`,`rental_rate`," +
		"`length`,`replacement_cost`,`rating`,`special_features`," +
		"`last_update`) VALUES "

	query := generateInsertStmt(table)
	tu.Equals(t, want, query)
}

func Test_getFieldNames(t *testing.T) {
	tests := []struct {
		name  string
		gets  []tableparser.Field
		wants []string
	}{
		{
			name: "should ignore auto_increment fields",
			gets: []tableparser.Field{
				{ColumnKey: "PRI", ColumnName: "test_autoincr", ColumnType: "int(11)", DataType: "int", Extra: "auto_increment"},
				{ColumnName: "test_int", ColumnType: "int(11)", DataType: "int"},
				{ColumnName: "test_datetime", ColumnType: "datetime(6)", DataType: "datetime"},
			},
			wants: []string{"`test_int`", "`test_datetime`"},
		},
		{
			name: "should ignore virtual generated columns",
			gets: []tableparser.Field{
				{ColumnName: "test_virtual", ColumnType: "int(11)", DataType: "int", Extra: "VIRTUAL GENERATED"},
				{ColumnName: "test_int", ColumnType: "int(11)", DataType: "int"},
				{ColumnName: "test_datetime", ColumnType: "datetime(6)", DataType: "datetime"},
			},
			wants: []string{"`test_int`", "`test_datetime`"},
		},
		{
			name: "should return field names with backticks",
			gets: []tableparser.Field{
				{ColumnName: "test_int", ColumnType: "int(11)", DataType: "int"},
				{ColumnName: "test_datetime", ColumnType: "datetime(6)", DataType: "datetime"},
			},
			wants: []string{"`test_int`", "`test_datetime`"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := getFieldNames(tc.gets)
			assert.Equal(t, tc.wants, res)
		})
	}
}

func Test_makeValueFuncs(t *testing.T) {
	tests := []struct {
		name  string
		gets  []tableparser.Field
		wants insertValues
	}{
		{
			name:  "should return nil when there are no value functions to be created",
			gets:  []tableparser.Field{},
			wants: nil,
		},
		{
			name: "should ignore virtual generated columns",
			gets: []tableparser.Field{
				{ColumnName: "test_virtual", ColumnType: "int(11)", DataType: "int", Extra: "VIRTUAL GENERATED"},
			},
			wants: nil,
		},
	}

	// TODO: This test is incomplete.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := makeValueFuncs(nil, tc.gets, 0)
			assert.Equal(t, tc.wants, res)
		})
	}
}
