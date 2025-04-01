package main

import (
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"os"
)

func generateArrowFile(filePath string) error {
	// 创建内存池
	pool := memory.NewGoAllocator()

	// 创建 Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// 创建 RecordBatch
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	ageBuilder := builder.Field(2).(*array.Int32Builder)

	// 动态添加数据
	// 动态添加更多的数据
	names := []string{"Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Hannah", "Isaac", "Jack",
		"Karen", "Liam", "Mona", "Nina", "Oliver", "Paul", "Quinn", "Rita", "Sam", "Tina"}

	for i, name := range names {
		idBuilder.Append(int32(i + 1))
		nameBuilder.Append(name)
		ageBuilder.Append(int32(20 + i*5)) // 示例年龄
	}

	// 完成记录并释放构建器
	record := builder.NewRecord()
	defer record.Release()

	// 写入文件
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

	// 写入 RecordBatch
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record batch: %v", err)
	}

	// 确保在关闭 Writer 之前不再使用
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %v", err)
	}

	fmt.Printf("Arrow file generated successfully: %s\n", filePath)
	return nil
}

func main() {
	filePath := "sample.arrow"
	if err := generateArrowFile(filePath); err != nil {
		fmt.Printf("Failed to generate Arrow file: %v\n", err)
	}
}
