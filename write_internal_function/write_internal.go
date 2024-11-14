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
	"io"
	"log"
	"os"
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

	request := &pb.WriterInternalDataRequest{
		ArrowBatch: buf.Bytes(),
		DbName:     "stream_task",
		TableName:  "test_data",
	}

	response := dataServiceClient.WriteInternalDBData(ctx, request)
	fmt.Println(response)
}

func serializeRecord(record arrow.Record) (*bytes.Buffer, error) { // 修改为值类型
	// 创建一个临时文件
	tempFile, err := os.CreateTemp("", "arrow-ipc-*.arrow")
	if err != nil {
		return nil, err
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	// 创建 Arrow IPC 写入器
	writer, err := ipc.NewFileWriter(tempFile, ipc.WithSchema(record.Schema()), ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	// 写入记录批次
	if err := writer.Write(record); err != nil {
		return nil, err
	}

	// 关闭写入器
	if err := writer.Close(); err != nil {
		return nil, err
	}

	// 读取临时文件内容
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, tempFile)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
