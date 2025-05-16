package main

import (
	"bytes"
	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
	"context"
	"github.com/apache/arrow/go/v15/arrow/ipc"
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
		AssetName:   "bigdatatest",
		ChainInfoId: 1,
		PlatformId:  1,
	}

	// 调用 ReadStream 方法
	readStream, err := dataServiceClient.ReadStream(ctx, request)
	if err != nil {
		log.Fatalf("Failed to read stream: %v", err)
	}

	bucketName := "data-service"
	objectName := "bigdatatest123456.arrow"

	// 创建OSS写入流
	writeStream, err := dataServiceClient.WriteOSSData(ctx, bucketName, objectName)
	if err != nil {
		log.Fatalf("Failed to create OSS write stream: %v", err)
	}

	// 循环边读边发
	for {
		response, err := readStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error receiving data: %v", err)
		}

		arrowBatch := response.GetArrowBatch()
		if len(arrowBatch) == 0 {
			log.Println("Received empty arrow batch.")
			continue
		}
		// 验证 Arrow 数据的完整性
		reader, err := ipc.NewReader(bytes.NewReader(arrowBatch))
		if err != nil {
			log.Fatalf("Invalid Arrow data: %v", err)
		}
		reader.Release()

		// 发送数据到写入流
		chunkRequest := &pb.OSSWriteRequest{
			BucketName: bucketName,
			ObjectName: objectName,
			Chunk:      arrowBatch, // 发送完整的 Arrow 批次
		}

		log.Printf("Sending chunk of size: %d bytes", len(chunkRequest.Chunk))

		if err := writeStream.Send(chunkRequest); err != nil {
			if err == io.EOF {
				log.Fatalf("Write stream closed unexpectedly: %v", err)
			}
			log.Fatalf("Failed to send data chunk: %v", err)
		}

		log.Println("Successfully sent chunk")
	}

	// 最后关闭写入流并获取响应
	finalResponse, err := writeStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Failed to close OSS stream: %v", err)
	}
	log.Printf("Final OSS write response: %v", finalResponse)

}
