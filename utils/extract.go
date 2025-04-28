package utils

import (
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/shopspring/decimal"
	"log"
	"time"
)

func ExtractRowData(record arrow.Record) ([][]interface{}, error) {
	var rows [][]interface{}
	numRows := record.NumRows() // 获取行数

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ { // 使用 int64
		rowData := []interface{}{}
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			column := record.Column(colIdx)
			var value interface{}

			switch column.DataType().ID() {
			case arrow.INT32:
				int32Array := column.(*array.Int32)
				value = int32Array.Value(int(rowIdx)) // 转换为 int
			case arrow.INT64:
				int64Array := column.(*array.Int64)
				value = int64Array.Value(int(rowIdx)) // 转换为 int64
			case arrow.STRING:
				stringArray := column.(*array.String)
				value = stringArray.Value(int(rowIdx)) // 转换为 string
			case arrow.FLOAT64:
				float64Array := column.(*array.Float64)
				value = float64Array.Value(int(rowIdx)) // 转换为 float64
			case arrow.DECIMAL128:
				decimal128Array := column.(*array.Decimal128)
				decimalValue := decimal128Array.Value(int(rowIdx))
				scale := decimal128Array.DataType().(*arrow.Decimal128Type).Scale
				bigInt := decimalValue.BigInt()

				// 根据 scale 调整小数点
				adjustment := decimal.New(1, int32(scale)) // 10^scale
				dec := decimal.NewFromBigInt(bigInt, 0).Div(adjustment)

				// 调试日志，查看中间值
				log.Printf("Decimal128 value: bigInt=%s, scale=%d, adjusted=%s", bigInt.String(), scale, dec.String())

				value = dec // 转换为 decimal.Decimal
			case arrow.DATE32:
				date32Array := column.(*array.Date32)
				daysSinceEpoch := date32Array.Value(int(rowIdx))
				timestamp := time.Unix(int64(daysSinceEpoch)*86400, 0).UTC()
				formattedDate := timestamp.Format("2006-01-02") // 格式化为 YYYY-MM-DD
				value = formattedDate
			case arrow.TIMESTAMP:
				timestampArray := column.(*array.Timestamp)
				timestampValue := timestampArray.Value(int(rowIdx))
				unit := timestampArray.DataType().(*arrow.TimestampType).Unit

				// 根据时间戳的单位进行转换
				switch unit {
				case arrow.Second:
					timestamp := time.Unix(int64(timestampValue), 0).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05")
					value = formattedTimestamp
				case arrow.Millisecond:
					timestamp := time.Unix(0, int64(timestampValue*1e6)).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05.000")
					value = formattedTimestamp
				case arrow.Microsecond:
					timestamp := time.Unix(0, int64(timestampValue*1e3)).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05.000000")
					value = formattedTimestamp
				case arrow.Nanosecond:
					timestamp := time.Unix(0, int64(timestampValue)).UTC()
					formattedTimestamp := timestamp.Format("2006-01-02 15:04:05.000000000")
					value = formattedTimestamp
				default:
					return nil, fmt.Errorf("unsupported timestamp unit: %v", unit)
				}
			// 可以根据需要添加更多类型的支持
			default:
				return nil, fmt.Errorf("unsupported column type: %v", column.DataType().ID())
			}

			rowData = append(rowData, value)
		}
		rows = append(rows, rowData)
	}
	return rows, nil
}
