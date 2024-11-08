/*
*

	@author: shiliang
	@date: 2024/11/8
	@note: oss操作

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

	bucketName := "data-service"
	objectName := "testdata.txt"
	filePath := "C:\\software\\go\\src\\test\\oss_function\\testdata.txt"

	response, err := dataServiceClient.WriteOSSData(ctx, bucketName, objectName, filePath)
	if err != nil {
		log.Fatalf("Failed to write OSS data: %v", err)
	}
	log.Printf("Response: %v", response)

	// 读oss数据

}
