/*
*

	@author: shiliang
	@date: 2024/11/12
	@note: 获取表数据量大小

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

	request := &pb.TableInfoRequest{
		AssetName:   "datatest-students",
		ChainInfoId: 1,
		PlatformId:  1,
	}

	response, err := dataServiceClient.GetTableInfo(ctx, request)
	if err != nil {
		log.Fatalf("Failed to read : %v", err)
	}
	log.Printf("Response: %v", response)

}
