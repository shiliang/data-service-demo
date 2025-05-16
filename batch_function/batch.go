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
	"log"
	"time"
)

func main() {
	ctx := context.Background()

	// 记录开始时间
	startTime := time.Now()

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
		ExecutorMemoryMB:              6072,
		ExecutorCores:                 4,
		DriverMemoryMB:                6536,
		DriverCores:                   2,
		Parallelism:                   200,
		NumPartitions:                 20,
	}

	/*request := &pb.BatchReadRequest{
		AssetName:   "datatest-students",
		ChainInfoId: 1,
		DbFields:    []string{"name"},
		PlatformId:  1,
		SparkConfig: sparkConfig,
	}*/

	request := &pb.BatchReadRequest{
		AssetName:   "bigdatatest",
		ChainInfoId: 1,
		PlatformId:  1,
		SparkConfig: sparkConfig,
		BucketName:  "data-service",
		DataObject:  "output.arrow",
		JoinColumns: []string{"id"},
		Mode:        pb.OperationMode_OPERATION_MODE_JOIN,
		TempTable:   "xwcfrfer",
	}

	// 1. 提交作业
	resp, err := dataServiceClient.SubmitBatchJob(ctx, request)
	if err != nil {
		log.Fatalf("提交作业失败: %v", err)
	}
	fmt.Println("作业已提交，初始状态：", resp.Status, "JobId:", request.RequestId)

	// 2. 轮询查询作业状态
	for {
		time.Sleep(5 * time.Second)
		statusResp, err := dataServiceClient.GetJobStatus(ctx, request.RequestId)
		if err != nil {
			log.Printf("查询作业状态失败: %v", err)
			continue
		}
		fmt.Println("当前作业状态：", statusResp.Status)
		if statusResp.Status == pb.JobStatus_JOB_STATUS_SUCCEEDED || statusResp.Status == pb.JobStatus_JOB_STATUS_FAILED {
			fmt.Println("作业完成，最终状态：", statusResp.Status)
			break
		}
	}
	// 记录结束时间
	endTime := time.Now()

	// 计算总时间
	totalTime := endTime.Sub(startTime)
	log.Printf("Total execution time: %v", totalTime)

}
