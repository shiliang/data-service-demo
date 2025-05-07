package main

import (
	"context"
	"io"
	"log"

	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
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
		AssetName:   "bigdatatest",
		ChainInfoId: 1,
		PlatformId:  1,
	}

	// 调用 ReadStream 方法
	stream, err := dataServiceClient.ReadStream(ctx, request)
	if err != nil {
		log.Fatalf("Failed to read stream: %v", err)
	}

	bucketName := "data-service"
	objectName := "bigdatatest123456.arrow"

	// 创建OSS写入流
	ossStream, err := dataServiceClient.WriteOSSData(ctx, bucketName, objectName, client.ArrowStreamFormat)
	if err != nil {
		log.Fatalf("Failed to create OSS write stream: %v", err)
	}

	for {
		response, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				// 读取流结束，关闭OSS写入流
				finalResponse, err := ossStream.CloseAndRecv()
				if err != nil {
					log.Fatalf("Failed to close OSS stream: %v", err)
				}
				log.Printf("Final OSS write response: %v", finalResponse)
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

		// 直接使用流发送数据
		chunkRequest := &pb.OSSWriteRequest{
			Chunk: arrowBatch,
		}
		if err := ossStream.Send(chunkRequest); err != nil {
			log.Fatalf("Failed to send data chunk: %v", err)
		}
		log.Printf("Sent chunk: %v", len(chunkRequest.Chunk))
	}

}
