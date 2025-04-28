package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"test/utils"

	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func generateArrowFile(filePath string) error {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	ageBuilder := builder.Field(2).(*array.Int32Builder)

	// 添加数据
	idBuilder.AppendValues([]int32{1, 2, 3}, nil)
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	ageBuilder.AppendValues([]int32{30, 25, 35}, nil)

	record := builder.NewRecord()
	defer record.Release()

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(pool))
	if err != nil {
		return fmt.Errorf("failed to create Arrow IPC writer: %v", err)
	}
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record batch: %v", err)
	}

	fmt.Printf("Arrow file generated successfully: %s\n", filePath)
	return nil
}

func validateArrowFile(filePath string) error {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// 读取文件的原始字节内容
	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file contents: %v", err)
	}

	magicBytes := content[:5]
	fmt.Printf("Magic Bytes: %s\n", string(magicBytes))

	// 打印文件内容的原始字节数和内容
	fmt.Printf("File size: %d bytes\n", len(content))
	fmt.Printf("Raw file content (first 256 bytes): %v\n", content[:256]) // 打印前256字节

	// 重置文件游标到文件开头
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to reset file pointer: %v", err)
	}

	// 使用 IPC 文件读取器解析数据
	pool := memory.NewGoAllocator()
	ipcReader, err := ipc.NewFileReader(file, ipc.WithAllocator(pool))
	if err != nil {
		return fmt.Errorf("failed to create Arrow IPC reader: %v", err)
	}

	// 读取所有记录批次并打印详细数据
	recordBatchIndex := 0
	for {
		// 读取一个记录批次
		recordBatch, err := ipcReader.Read()
		if err == io.EOF {
			// 所有数据已读取完毕
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record batch: %v", err)
		}

		// 打印记录批次的基本信息
		fmt.Printf("Record batch %d has %d rows and %d columns.\n", recordBatchIndex, recordBatch.NumRows(), recordBatch.NumCols())

		// 遍历每一列数据
		for colIdx := int64(0); colIdx < int64(recordBatch.NumCols()); colIdx++ {
			// 获取当前列数据
			col := recordBatch.Column(int(colIdx))

			// 打印列名称和列的每一行数据
			fmt.Printf("Column %d: %s\n", colIdx, recordBatch.Schema().Field(int(colIdx)).Name)
			for rowIdx := int64(0); rowIdx < int64(recordBatch.NumRows()); rowIdx++ {
				// 打印每行数据
				switch c := col.(type) {
				case *array.Int32:
					fmt.Printf("Row %d: %d\n", rowIdx, c.Value(int(rowIdx)))
				case *array.String:
					fmt.Printf("Row %d: %s\n", rowIdx, c.Value(int(rowIdx)))
				case *array.Float64:
					fmt.Printf("Row %d: %f\n", rowIdx, c.Value(int(rowIdx)))
				case *array.Int64:
					fmt.Printf("Row %d: %d\n", rowIdx, c.Value(int(rowIdx)))
				default:
					fmt.Printf("Row %d: Unknown column type\n", rowIdx)
				}
			}
		}

		// 增加记录批次索引
		recordBatchIndex++
	}

	fmt.Println("Arrow file validation completed successfully.")
	return nil
}

func main() {
	filePath := "D:\\code\\demo\\data-service-demo\\funcinternal\\bytedata.arrow"
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

	// 读取文件内容
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	fmt.Printf("File size: %d bytes\n", len(data))
	fmt.Printf("First 16 bytes: %v\n", data[:16])
	// 写入OSS
	bucketName := "data-service"
	objectName := "bytedata.arrow"
	response, err := dataServiceClient.WriteOSSData(ctx, bucketName, objectName, client.ArrowStreamFormat, data)
	if err != nil {
		log.Fatalf("Failed to write OSS data: %v", err)
	}
	fmt.Printf("Response: %v\n", response)

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// 创建 Arrow 内存分配器
	allocator := memory.NewGoAllocator()

	// 创建 Arrow Reader
	reader, err := ipc.NewFileReader(file, ipc.WithAllocator(allocator))
	if err != nil {
		log.Fatalf("Failed to create Arrow reader: %v", err)
	}

	fmt.Println("开始读取文件...")

	// 读取每一条记录
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read record: %v", err)
		}
		defer record.Release()

		// 打印 Record 的 schema 信息
		fmt.Println("Record schema:", record.Schema())
		fmt.Printf("Record has %d rows and %d columns\n", record.NumRows(), record.NumCols())

		// 提取并打印每一行的数据
		rows, err := utils.ExtractRowData(record)
		if err != nil {
			log.Fatalf("Error extracting row data: %v", err)
		}

		fmt.Println("提取到的数据：")
		for rowIndex, row := range rows {
			fmt.Printf("Row %d: ", rowIndex+1)
			for colIndex, value := range row {
				fmt.Printf("%s=%v ", record.ColumnName(colIndex), value)
			}
			fmt.Println()
		}
	}
}
