package main

import (
	"context"
	"io"
	"log"
	"time"

	client "chainweaver.org.cn/chainweaver/mira/mira-data-service-client"
	pb "chainweaver.org.cn/chainweaver/mira/mira-data-service-client/proto/datasource"
)

func main() {
	// 创建读取流的上下文，设置更长的超时时间
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer readCancel()

	// 创建写入流的上下文，设置更长的超时时间
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer writeCancel()

	// 创建一个ServerInfo实例
	serverInfo := &pb.ServerInfo{
		//Namespace:   "mira1", // 如果在Kubernetes集群中使用，可以指定命名空间
		ServiceName: "192.168.40.243",
		ServicePort: "30015",
	}

	dataServiceClient, err := client.NewDataServiceClient(readCtx, serverInfo)
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
	stream, err := dataServiceClient.ReadStream(readCtx, request)
	if err != nil {
		log.Fatalf("Failed to read stream: %v", err)
	}

	bucketName := "data-service"
	objectName := "bigdatatest123456.arrow"

	// 创建OSS写入流
	ossStream, err := dataServiceClient.WriteOSSData(writeCtx, bucketName, objectName, client.ArrowStreamFormat)
	if err != nil {
		log.Fatalf("Failed to create OSS write stream: %v", err)
	}

	for {
		select {
		case <-readCtx.Done():
			log.Printf("Read context canceled: %v", readCtx.Err())
			return
		case <-writeCtx.Done():
			log.Printf("Write context canceled: %v", writeCtx.Err())
			return
		default:
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					// 读取流结束，关闭OSS写入流
					finalResponse, err := ossStream.CloseAndRecv()
					if err != nil {
						log.Printf("Failed to close OSS stream: %v", err)
					} else {
						log.Printf("Final OSS write response: %v", finalResponse)
					}
					return
				}
				log.Printf("Error receiving data: %v", err)
				return
			}

			// 解码 arrow_batch 数据
			arrowBatch := response.GetArrowBatch()
			if len(arrowBatch) == 0 {
				log.Println("Received empty arrow batch.")
				continue
			}

			// 使用流发送数据
			chunkRequest := &pb.OSSWriteRequest{
				Chunk: arrowBatch,
			}
			if err := ossStream.Send(chunkRequest); err != nil {
				log.Printf("Failed to send data chunk: %v", err)
				return
			}
			log.Printf("Sent chunk: %v", len(chunkRequest.Chunk))
		}
	}
}
