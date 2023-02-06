package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
)

type CSVHelper struct {
	Name    string
	reader  *csv.Reader
	headers []string
	records [][]string
}

var (
	errEmptyRecords = errors.New("empty records")
)

func NewCSVHelper(name, file string) (*CSVHelper, error) {
	fileInput, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	c := &CSVHelper{
		Name:   name,
		reader: csv.NewReader(fileInput),
	}

	err = c.init()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func newCSV(name string) *CSVHelper {
	return &CSVHelper{
		Name: name,
	}
}

func (c *CSVHelper) clone() *CSVHelper {
	c1 := newCSV(c.Name)
	c1.headers = make([]string, len(c.headers))
	copy(c1.headers, c.headers)

	c1.records = make([][]string, len(c.records))
	for i, v := range c.records {
		c1.records[i] = make([]string, len(v))
		copy(c1.records[i], v)
	}
	return c1
}

func (c *CSVHelper) init() error {
	all, err := c.reader.ReadAll()
	if err != nil {
		return err
	}

	if len(all) < 2 {
		return errEmptyRecords
	}

	c.headers = all[0]
	c.records = all[1:]
	return nil
}

// TODO : check csv format, especially the length of headers and records
func (c *CSVHelper) check() error {
	return nil
}

type MapColumn struct {
	Src     string
	Dst     string
	MapFunc func(string) string
}

func (c *CSVHelper) MapColumns(columns ...*MapColumn) *CSVHelper {
	c1 := c.clone()

	colIndexMap := make(map[int]*MapColumn)
	for i, v := range c1.headers {
		for _, column := range columns {
			if v == column.Src {
				c1.headers[i] = column.Dst
				colIndexMap[i] = column
				break
			}
		}
	}

	for _, record := range c1.records {
		for i, v := range record {
			if column, ok := colIndexMap[i]; ok {
				record[i] = column.MapFunc(v)
			}
		}
	}

	return c1
}

func (c *CSVHelper) ToOutput(filePath string) error {

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}

	w := csv.NewWriter(f)

	err = w.Write(c.headers)
	if err != nil {
		return err
	}

	err = w.WriteAll(c.records)
	if err != nil {
		return err
	}

	return nil
}

func (c *CSVHelper) Join(right *CSVHelper, leftColumn, rightColumn string) *CSVHelper {
	c1 := newCSV(c.Name + "." + right.Name)

	headerMap := make(map[string]bool)
	for _, v := range c.headers {
		headerMap[v] = true
	}

	rightHeader := make([]string, len(right.headers))
	copy(rightHeader, right.headers)

	rightIndex := -1
	leftIndex := -1
	for i, v := range rightHeader {
		if v == rightColumn {
			rightIndex = i
			break
		}
	}

	for i, v := range c.headers {
		if v == leftColumn {
			leftIndex = i
			break
		}
	}

	if rightIndex == -1 || leftIndex == -1 {
		return c
	}

	for i, v := range rightHeader {
		if headerMap[v] {
			rightHeader[i] = right.Name + "." + v
		}
	}

	c1.headers = append(c.headers, rightHeader...)

	rightRecordsMap := make(map[string][]string)
	for _, record := range right.records {
		rightRecordsMap[record[rightIndex]] = record
	}

	for _, record := range c.records {
		key := record[leftIndex]
		if rightRecords, ok := rightRecordsMap[key]; ok {
			c1.records = append(c1.records, append(record, rightRecords...))
		} else {
			c1.records = append(c1.records, record)
		}
	}

	return c1
}

func (c *CSVHelper) Select(fields []string) *CSVHelper {
	c1 := newCSV(c.Name)

	fieldIndexMap := make(map[int]bool)
	for index, header := range c.headers {
		for _, field := range fields {
			if header == field {
				fieldIndexMap[index] = true
				c1.headers = append(c1.headers, header)
				break
			}
		}
	}

	if len(fieldIndexMap) == 0 {
		return c1
	}

	for _, record := range c.records {
		newRecord := make([]string, 0)
		for i, v := range record {
			if _, ok := fieldIndexMap[i]; ok {
				newRecord = append(newRecord, v)
			}
		}
		c1.records = append(c1.records, newRecord)
	}

	return c1
}

// TODO pretty print
func (c *CSVHelper) Print() string {
	for _, v := range c.headers {
		fmt.Printf("%s\t", v)
	}
	fmt.Println()

	for _, record := range c.records {
		for _, v := range record {
			fmt.Printf("%s\t", v)
		}
		fmt.Println()
	}
	return ""
}
