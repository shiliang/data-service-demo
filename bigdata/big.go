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

	// 收集所有数据
	var allData []byte
	for {
		response, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error receiving data: %v", err)
		}

		// 获取 arrow_batch 数据
		arrowBatch := response.GetArrowBatch()
		if len(arrowBatch) == 0 {
			log.Println("Received empty arrow batch.")
			continue
		}

		// 将数据追加到总数据中
		allData = append(allData, arrowBatch...)
		log.Printf("Received chunk: %v bytes", len(arrowBatch))
	}

	log.Printf("Total data received: %v bytes", len(allData))

	// 写入OSS
	bucketName := "data-service"
	objectName := "bigdatatest123456.arrow"

	// 创建OSS写入流
	ossStream, err := dataServiceClient.WriteOSSData(ctx, bucketName, objectName, client.ArrowStreamFormat)
	if err != nil {
		log.Fatalf("Failed to create OSS write stream: %v", err)
	}

	// 发送数据
	if err := ossStream.Send(&pb.OSSWriteRequest{
		Chunk: allData,
	}); err != nil {
		log.Fatalf("Failed to send data: %v", err)
	}

	// 关闭流并获取响应
	writeResponse, err := ossStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Failed to close stream: %v", err)
	}

	log.Printf("Write response: %v", writeResponse)
}
