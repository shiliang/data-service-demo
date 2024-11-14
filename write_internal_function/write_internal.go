/*
*

	@author: shiliang
	@date: 2024/11/14
	@note:

*
*/
package main

import (
	"bytes"
	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"log"
)

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

	// 创建示例数据
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	// 创建一个记录批次
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	// 添加示例数据
	builder.Field(0).(*array.Int64Builder).Append(99999)
	builder.Field(1).(*array.StringBuilder).Append("example data")

	record := builder.NewRecord()
	defer record.Release()

	// 将记录批次序列化为 Arrow IPC 格式
	buf, err := serializeRecord(record)
	if err != nil {
		log.Fatalf("failed to serialize record: %v", err)
	}
	fmt.Printf("Serialized record size: %d bytes\n", buf.Len())

	request := &pb.WriterInternalDataRequest{
		ArrowBatch: buf.Bytes(),
		DbName:     "stream_task",
		TableName:  "defrgt",
	}

	response := dataServiceClient.WriteInternalDBData(ctx, request)
	fmt.Println(response)
}

func serializeRecord(record arrow.Record) (*bytes.Buffer, error) {
	// 创建一个字节缓冲区用于存储序列化的 IPC 数据
	buf := new(bytes.Buffer)

	// 创建 Arrow IPC 文件写入器
	writer := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()), ipc.WithAllocator(memory.DefaultAllocator))
	if writer == nil {
		return nil, fmt.Errorf("failed to create IPC writer")
	}
	defer writer.Close()

	// 将记录写入到 IPC 文件
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write record to IPC: %v", err)
	}

	// 关闭写入器
	if err := writer.Close(); err != nil {
		log.Fatalf("Failed to close IPC writer: %v", err)
	}

	// 返回包含 IPC 数据的字节缓冲区
	return buf, nil
}
