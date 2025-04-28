/*
*

	@author: shiliang
	@date: 2024/11/8
	@note: oss操作

*
*/
package main

import (
	"bytes"
	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"log"
	"test/utils"
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
	//// 写入oss
	//bucketName := "data-service"
	//objectName := "bytedata.txt"
	//filePath := "C:\\software\\go\\src\\test\\oss_function\\00d22d1015704b1ebf46e73a2e2235d7.arrow"
	//data1 := []byte("some data")
	//response, err := dataServiceClient.WriteOSSData(ctx, bucketName, objectName, filePath, data1)
	//if err != nil {
	//	log.Fatalf("Failed to write OSS data: %v", err)
	//}
	//log.Printf("Response: %v", response)

	// 读oss数据
	request := &pb.OSSReadRequest{
		BucketName: "data-service",
		ObjectName: "data/ab58867b-dcd8-47bd-ab96-36324abf0ba6_partition_102995875df440ffa1e19a43f1401ef5.arrow",
	}

	stream, err := dataServiceClient.ReadOSSData(ctx, request)
	if err != nil {
		log.Fatalf("Failed to read stream: %v", err)
	}

	// 处理流式数据
	for {
		response, err := stream.Recv()
		if err != nil {
			log.Fatalf("Error receiving data: %v", err)
		}

		// 解码 arrow_batch 数据
		arrowBatch := response.Chunk // 获取 arrow_batch 字段
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
			rows, err := utils.ExtractRowData(record)
			if err != nil {
				log.Fatalf("Error extracting row data: %v", err)
			}

			for rowIndex, row := range rows {
				fmt.Printf("Row %d: ", rowIndex+1)
				for colIndex, value := range row {
					fmt.Printf("%s=%v ", record.ColumnName(colIndex), value)
				}
				fmt.Println()
			}
		}
	}

}
