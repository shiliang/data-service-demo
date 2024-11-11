/*
*

	@author: shiliang
	@date: 2024/11/8
	@note:

*
*/
package main

import (
	"bytes"
	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
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
	// 创建内存分配器
	pool := memory.NewGoAllocator()

	// 定义 Arrow 表的 schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	// 创建 RecordBuilder 用于填充数据
	recordBuilder := array.NewRecordBuilder(pool, schema)
	defer recordBuilder.Release()

	// 向 RecordBuilder 中添加数据
	recordBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	recordBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"shi", "liang"}, nil)

	// 生成 RecordBatch
	recordBatch := recordBuilder.NewRecord()
	defer recordBatch.Release()

	// 使用 IPC 写入器将 RecordBatch 写入到字节缓冲区中
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	defer writer.Close()

	// 将 RecordBatch 写入 IPC 流
	if err := writer.Write(recordBatch); err != nil {
		log.Fatalf("Failed to write RecordBatch to IPC buffer: %v", err)
	}

	// 获取序列化的 Arrow 批次字节数据
	arrowBatchBytes := buffer.Bytes()

	request := &pb.WriterExternalDataRequest{
		ArrowBatch:  arrowBatchBytes,
		PlatformId:  1,
		AssetName:   "datatest-students",
		TableName:   "students222",
		ChainInfoId: 1,
	}
	response, err := dataServiceClient.WriteExternalDBData(ctx, request)

	if err != nil {
		log.Fatalf("Failed to write external data: %v", err)
	}
	log.Printf("Response: %v", response)
}
