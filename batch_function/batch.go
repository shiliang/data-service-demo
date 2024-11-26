/*
*

	@author: shiliang
	@date: 2024/11/7
	@note: 批处理接口使用示例

*
*/
package main

import (
	"bytes"
	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow/array"
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

	// 创建 SparkConfig 实例
	sparkConfig := &pb.SparkConfig{
		DynamicAllocationEnabled:      true,
		DynamicAllocationMinExecutors: 2,
		DynamicAllocationMaxExecutors: 10,
		ExecutorMemoryMB:              3072,
		ExecutorCores:                 4,
		DriverMemoryMB:                3536,
		DriverCores:                   2,
	}

	/*request := &pb.BatchReadRequest{
		AssetName:   "datatest-students",
		ChainInfoId: 1,
		DbFields:    []string{"name"},
		PlatformId:  1,
		SparkConfig: sparkConfig,
	}*/

	request := &pb.BatchReadRequest{
		AssetName:   "datatest",
		ChainInfoId: 1,
		DbFields:    []string{"tcol01"},
		PlatformId:  1,
		SparkConfig: sparkConfig,
	}

	// 调用 ReadBatchData 方法获取流式数据
	stream, err := dataServiceClient.ReadBatchData(ctx, request)
	if err != nil {
		log.Fatalf("failed to call ReadBatchData: %v", err)
	}

	// 遍历流式响应数据
	for {
		// 从流中接收 OSSReadResponse
		response, err := stream.Recv()
		if err == io.EOF {
			// 所有数据已接收完毕
			break
		}
		if err != nil {
			log.Fatalf("failed to receive data from stream: %v", err)
		}

		arrowBatch := response.ArrowBatch
		// 使用 Arrow IPC Reader 反序列化
		reader, err := ipc.NewReader(bytes.NewReader(arrowBatch), ipc.WithAllocator(memory.DefaultAllocator))
		if err != nil {
			log.Fatalf("failed to create Arrow IPC Reader: %v", err)
		}
		defer reader.Release()

		// 遍历每个批次的数据表
		for reader.Next() {
			record := reader.Record()
			defer record.Release()

			// 打印表结构
			log.Printf("Schema: %s", record.Schema())

			// 遍历行
			for i := 0; i < int(record.NumRows()); i++ {
				row := []interface{}{}
				for j := 0; j < int(record.NumCols()); j++ {
					column := record.Column(j)
					switch col := column.(type) {
					case *array.Int32:
						row = append(row, col.Value(i))
					case *array.String:
						row = append(row, col.Value(i))
					default:
						row = append(row, "unknown type")
					}
				}
				log.Printf("Row %d: %v", i, row)
			}
		}
	}

	fmt.Println("Data streaming completed successfully.")
}
