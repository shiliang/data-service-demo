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

	request := &pb.BatchReadRequest{
		AssetName:   "datatest-students",
		ChainInfoId: 1,
		DbFields:    []string{"name"},
		RequestId:   "8a5ccd24-aa46-4fae-ad0f-b4d241a850f9",
		PlatformId:  1,
	}
}
