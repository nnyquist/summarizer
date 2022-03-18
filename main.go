package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/nnyquist/summarizer/colstats"
	"github.com/nnyquist/summarizer/linecount"
)

func main() {
	numWorkers := runtime.NumCPU()

	// Open and defer the file close
	file, err := os.Open(`./data/sdud_2019.csv`)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// row count
	fileReader := &linecount.FileReader{
		File: file,
	}
	lines := make(chan int)

	for i := 0; i < numWorkers; i++ {
		go linecount.FileReaderCounter(fileReader, lines)
	}

	totalRecords := 0
	for i := 0; i < numWorkers; i++ {
		count := <-lines
		totalRecords += count
	}
	close(lines)

	// column reader
	file, err = os.Open(`./data/sdud_2019.csv`)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	r := csv.NewReader(file)
	columns := make(colstats.ColumnKey)

	colstats.GetColumnStats(r, columns, true, 2)

	// table output for column stats
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"#", "Column Name", "Max Len", "Min Len", "Avg Len",
		"% Numeric", "Numeric Total", "% Date"})
	for k, v := range columns {
		name := &v.ColumnName
		max := &v.MaxLen
		min := &v.MinLen
		avg := fmt.Sprintf("%.2f", v.AggregateLen/float64(totalRecords))
		isN := fmt.Sprintf("%.f%%",
			(float64(v.IsNumeric)/float64(totalRecords))*100)
		isD := fmt.Sprintf("%.f%%",
			(float64(v.IsDate)/float64(totalRecords))*100)
		numericTotal := fmt.Sprintf("%.2f", v.TotalNumeric)
		t.AppendRow([]interface{}{k, *name, *max, *min, avg, isN, numericTotal, isD})
	}
	t.Render()

	// table output for total records
	t = table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"File Name", "Total Records"})
	t.AppendRow([]interface{}{file.Name(), totalRecords})
	t.Render()
}
