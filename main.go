package main

import (
	"bytes"
	"chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"io"
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

	// 创建StreamReadRequest实例
	request := &pb.StreamReadRequest{
		AssetName:   "datatest-students",
		ChainInfoId: 1,
		DbFields:    []string{"name"},
		RequestId:   "8a5ccd24-aa46-4fae-ad0f-b4d241a850f9",
		PlatformId:  1,
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
			if err == io.EOF {
				// 流结束
				break
			}
			log.Fatalf("Error receiving data: %v", err)
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

		// 打印每个 Record
		for reader.Next() {
			record := reader.Record()
			fmt.Printf("Received Arrow Record: %v\n", record)
		}
	}

}
