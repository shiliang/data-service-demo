package main

import (
	"chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"io"
	"log"
	"os"
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
	client.NewDataServiceClient()
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

	fmt.Println("File is a valid Arrow IPC file.")
	fmt.Println("Schema:", ipcReader.Schema()) // 打印 Schema 信息

	// 读取所有记录批次
	for i := 0; i < ipcReader.NumRecords(); i++ {
		record, err := ipcReader.Record(i)
		if err != nil {
			return fmt.Errorf("failed to read record batch %d: %v", i, err)
		}
		fmt.Printf("Record batch %d has %d rows and %d columns.\n", i, record.NumRows(), record.NumCols())
	}

	return nil
}

func main() {
	filePath := "C:\\software\\go\\src\\test\\funcinternal\\file.arrow"
	/*if err := generateArrowFile(filePath); err != nil {
		log.Fatalf("Failed to generate Arrow file: %v", err)
	}*/

	if err := validateArrowFile(filePath); err != nil {
		log.Fatalf("Failed to validate Arrow IPC file: %v", err)
	}

	fmt.Println("All record batches processed successfully.")
}
