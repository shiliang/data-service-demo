/*
*

	@author: shiliang
	@date: 2024/11/7
	@note: 流式读取接口调用示例

*
*/
package main

import (
	"bytes"
	"chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"log"
)

// ExtractRowData 从 Arrow Record 中提取每一行的数据
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
				value = decimalValue.ToFloat64(decimal128Array.DataType().(*arrow.Decimal128Type).Scale) // 转换为 float64
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

func main() {
	ctx := context.Background()

	// 创建一个ServerInfo实例
	serverInfo := &pb.ServerInfo{
		//Namespace:   "mira1", // 如果在Kubernetes集群中使用，可以指定命名空间
		ServiceName: "192.168.40.243",
		ServicePort: "30015",
	}
	dataServiceClient, err := client.NewDataServiceClient(ctx, serverInfo)
	if err != nil {
		log.Fatalf("failed to initialize DataServiceClient: %v", err)
	}

	// 创建排序规则
	sortRules := []*pb.SortRule{
		{FieldName: "score", SortOrder: pb.SortOrder_ASC},
	}

	// 创建StreamReadRequest实例
	request := &pb.StreamReadRequest{
		AssetName:       "datatest",
		ChainInfoId:     1,
		DbFields:        []string{"tcol14"},
		PlatformId:      1,
		SortRules:       sortRules,
		FilterNames:     []string{"tcol08"}, // 指定过滤条件
		FilterOperators: []pb.FilterOperator{pb.FilterOperator_IN_OPERATOR},
		FilterValues: []*pb.FilterValue{
			//{
			//	StrValues: []string{"Alice"},
			//},
			{
				FloatValues: []float64{1.46},
			},
		},
	}

	// 调用 ReadStream 方法
	stream, err := dataServiceClient.ReadStream(ctx, request)
	if err != nil {
		log.Fatalf("Failed to read stream: %v", err)
	}

	// 处理流式数据
	for {
		response, err := stream.Recv()
		if err != nil {
			log.Fatalf("Error receiving data: %v", err)
		}
		if string(response.ArrowBatch) == "EOF" {
			log.Println("Received EOF.")
			break
		}

		// 解码 arrow_batch 数据
		arrowBatch := response.GetArrowBatch() // 获取 arrow_batch 字段
		if len(arrowBatch) == 0 {
			log.Println("Received empty arrow batch.")
			continue
		}

		// 创建 Arrow 内存分配器
		allocator := memory.NewGoAllocator()
		reader, err := ipc.NewReader(bytes.NewReader(arrowBatch), ipc.WithAllocator(allocator))
		if err != nil {
			log.Fatalf("Failed to create Arrow reader: %v", err)
		}
		defer reader.Release()

		for reader.Next() {
			record := reader.Record()
			defer record.Release()

			// 打印 Record 的 schema 信息
			fmt.Println("Record schema:", record.Schema())

			// 提取并打印每一行的数据
			rows, err := ExtractRowData(record)
			if err != nil {
				log.Fatalf("Error extracting row data: %v", err)
			}

			for _, row := range rows {
				fmt.Printf("Row: %v\n", row)
			}
		}
	}

}
