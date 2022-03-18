package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/nnyquist/summarizer/linecount"
)

type ColumnInfo struct {
	Position, MaxLen, MinLen int
	AggregateLen             float64
	IsNumeric, IsDate        uint
	TotalNumeric             float64
}

func main() {
	const dateFormat = "1/2/2006" // could we get this format from the user?
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
	var colNames []string
	columnMap := make(map[string]ColumnInfo)

	i := 1
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		// assuming header row
		if i == 1 {
			colNames = record
			i++
			continue
		}
		for j, val := range record {
			k := colNames[j]
			columnData := columnMap[k]
			colLen := len(val)

			if colLen > columnData.MaxLen {
				columnData.MaxLen = colLen
			}

			if i == 2 || colLen < columnData.MinLen {
				columnData.MinLen = colLen
			}

			columnData.AggregateLen += float64(colLen)

			floatVal, err := strconv.ParseFloat(val, 64)
			if err != nil {
				columnData.IsNumeric += 0
			} else {
				columnData.IsNumeric += 1
				columnData.TotalNumeric += floatVal
			}

			_, err = time.Parse(dateFormat, val)
			if err != nil {
				columnData.IsDate += 0
			} else {
				columnData.IsDate += 1
			}

			columnMap[k] = columnData
		}
		i++
	}

	for i, k := range colNames {
		columnData := columnMap[k]
		columnData.Position = i + 1
		columnMap[k] = columnData
	}

	fmt.Println(totalRecords, file.Name())

	// table output
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"#", "Column Name", "Max Len", "Min Len", "Avg Len",
		"% Numeric", "Numeric Total", "% Date"})
	for _, k := range colNames {
		v := columnMap[k]
		pos := &v.Position
		max := &v.MaxLen
		min := &v.MinLen
		avg := fmt.Sprintf("%.2f", v.AggregateLen/float64(totalRecords))
		isN := fmt.Sprintf("%.f%%",
			(float64(v.IsNumeric)/float64(totalRecords))*100)
		isD := fmt.Sprintf("%.f%%",
			(float64(v.IsDate)/float64(totalRecords))*100)
		numericTotal := fmt.Sprintf("%.2f", v.TotalNumeric)
		t.AppendRow([]interface{}{*pos, k, *max, *min, avg, isN, numericTotal, isD})
	}
	t.Render()
}
