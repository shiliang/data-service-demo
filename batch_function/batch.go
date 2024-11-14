/*
*

	@author: shiliang
	@date: 2024/11/7
	@note: 批处理接口使用示例

*
*/
package main

import (
	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
	"fmt"
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
		ExecutorMemoryMB:              5096,
		ExecutorCores:                 4,
		DriverMemoryMB:                3048,
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

		// 处理接收到的 OSSReadResponse 数据
		fmt.Printf("Received chunk: %s\n", response.Chunk)
	}

	fmt.Println("Data streaming completed successfully.")
}
