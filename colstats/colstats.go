package colstats

import (
	"encoding/csv"
	"io"
	"strconv"
	"time"
)

type ColumnInfo struct {
	ColumnName        string
	MaxLen, MinLen    int
	AggregateLen      float64
	IsNumeric, IsDate uint
	TotalNumeric      float64
}

type ColumnKey map[int]ColumnInfo

func GetColumnStats(r *csv.Reader, c ColumnKey, header bool, firstRow int) {
	const dateFormat = "1/2/2006" // could we get this format from the user?

	i := 1
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		// Extract Header Info if Present
		if i == 1 && header {
			for j, val := range record {
				colData := c[j+1]
				colData.ColumnName = val
				c[j+1] = colData
			}
			i++
			continue
		}
		for j, val := range record {
			colLen := len(val)
			colData := c[j+1]

			// Calculate max column length
			if colLen > colData.MaxLen {
				colData.MaxLen = colLen
			}

			// Calculate min column length
			if i == firstRow || colLen < colData.MinLen {
				colData.MinLen = colLen
			}

			// Aggregate column lengths
			colData.AggregateLen += float64(colLen)

			// Evaluate numeric type
			floatVal, err := strconv.ParseFloat(val, 64)
			if err != nil {
				colData.IsNumeric += 0
			} else {
				colData.IsNumeric += 1
				colData.TotalNumeric += floatVal
			}

			// Evaluate date type
			_, err = time.Parse(dateFormat, val)
			if err != nil {
				colData.IsDate += 0
			} else {
				colData.IsDate += 1
			}

			c[j+1] = colData
		}
		i++
	}
}
