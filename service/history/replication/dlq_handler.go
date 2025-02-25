// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination dlq_handler_mock.go

package replication

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

var (
	errInvalidCluster = &serviceerror.InvalidArgument{Message: "Invalid target cluster name."}
)

type (
	// DLQHandler is the interface handles replication DLQ messages
	DLQHandler interface {
		GetMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
			pageSize int,
			pageToken []byte,
		) ([]*replicationspb.ReplicationTask, []byte, error)
		PurgeMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
		) error
		MergeMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
			pageSize int,
			pageToken []byte,
		) ([]byte, error)
	}

	dlqHandlerImpl struct {
		taskExecutorsLock sync.Mutex
		taskExecutors     map[string]TaskExecutor
		shard             shard.Context
		deleteManager     workflow.DeleteManager
		workflowCache     workflow.Cache
		logger            log.Logger
	}
)

func NewLazyDLQHandler(
	shard shard.Context,
	deleteManager workflow.DeleteManager,
	workflowCache workflow.Cache,
) DLQHandler {
	return newDLQHandler(
		shard,
		deleteManager,
		workflowCache,
		make(map[string]TaskExecutor),
	)
}

func newDLQHandler(
	shard shard.Context,
	deleteManager workflow.DeleteManager,
	workflowCache workflow.Cache,
	taskExecutors map[string]TaskExecutor,
) *dlqHandlerImpl {

	if taskExecutors == nil {
		panic("Failed to initialize replication DLQ handler due to nil task executors")
	}
	return &dlqHandlerImpl{
		shard:         shard,
		deleteManager: deleteManager,
		workflowCache: workflowCache,
		taskExecutors: taskExecutors,
		logger:        shard.GetLogger(),
	}
}

func (r *dlqHandlerImpl) GetMessages(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {

	tasks, _, token, err := r.readMessagesWithAckLevel(
		ctx,
		sourceCluster,
		lastMessageID,
		pageSize,
		pageToken,
	)
	return tasks, token, err
}

func (r *dlqHandlerImpl) PurgeMessages(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
) error {

	ackLevel := r.shard.GetReplicatorDLQAckLevel(sourceCluster)
	err := r.shard.GetExecutionManager().RangeDeleteReplicationTaskFromDLQ(
		ctx,
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
				ShardID:             r.shard.GetShardID(),
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.Key{TaskID: ackLevel + 1},
				ExclusiveMaxTaskKey: tasks.Key{TaskID: lastMessageID + 1},
			},
			SourceClusterName: sourceCluster,
		},
	)
	if err != nil {
		return err
	}

	if err = r.shard.UpdateReplicatorDLQAckLevel(
		sourceCluster,
		lastMessageID,
	); err != nil {
		r.logger.Error("Failed to purge history replication message", tag.Error(err))
		// The update ack level should not block the call. Ignore the error.
	}
	return nil
}

func (r *dlqHandlerImpl) MergeMessages(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]byte, error) {

	replicationTasks, ackLevel, token, err := r.readMessagesWithAckLevel(
		ctx,
		sourceCluster,
		lastMessageID,
		pageSize,
		pageToken,
	)

	taskExecutor, err := r.getOrCreateTaskExecutor(sourceCluster)
	if err != nil {
		return nil, err
	}

	for _, task := range replicationTasks {
		if _, err := taskExecutor.Execute(
			task,
			true,
		); err != nil {
			return nil, err
		}
	}

	err = r.shard.GetExecutionManager().RangeDeleteReplicationTaskFromDLQ(
		ctx,
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
				ShardID:             r.shard.GetShardID(),
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.Key{TaskID: ackLevel + 1},
				ExclusiveMaxTaskKey: tasks.Key{TaskID: lastMessageID + 1},
			},
			SourceClusterName: sourceCluster,
		},
	)
	if err != nil {
		return nil, err
	}

	if err = r.shard.UpdateReplicatorDLQAckLevel(
		sourceCluster,
		lastMessageID,
	); err != nil {
		r.logger.Error("Failed to purge history replication message", tag.Error(err))
		// The update ack level should not block the call. Ignore the error.
	}
	return token, nil
}

func (r *dlqHandlerImpl) readMessagesWithAckLevel(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, int64, []byte, error) {

	ackLevel := r.shard.GetReplicatorDLQAckLevel(sourceCluster)
	resp, err := r.shard.GetExecutionManager().GetReplicationTasksFromDLQ(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             r.shard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.Key{TaskID: ackLevel + 1},
			ExclusiveMaxTaskKey: tasks.Key{TaskID: lastMessageID + 1},
			BatchSize:           pageSize,
			NextPageToken:       pageToken,
		},
		SourceClusterName: sourceCluster,
	})
	if err != nil {
		return nil, ackLevel, nil, err
	}
	pageToken = resp.NextPageToken

	remoteAdminClient := r.shard.GetRemoteAdminClient(sourceCluster)
	taskInfo := make([]*replicationspb.ReplicationTaskInfo, 0, len(resp.Tasks))
	for _, task := range resp.Tasks {
		switch task := task.(type) {
		case *tasks.SyncActivityTask:
			taskInfo = append(taskInfo, &replicationspb.ReplicationTaskInfo{
				NamespaceId:  task.NamespaceID,
				WorkflowId:   task.WorkflowID,
				RunId:        task.RunID,
				TaskType:     enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
				TaskId:       task.TaskID,
				Version:      task.GetVersion(),
				FirstEventId: 0,
				NextEventId:  0,
				ScheduledId:  task.ScheduledID,
			})
		case *tasks.HistoryReplicationTask:
			taskInfo = append(taskInfo, &replicationspb.ReplicationTaskInfo{
				NamespaceId:  task.NamespaceID,
				WorkflowId:   task.WorkflowID,
				RunId:        task.RunID,
				TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
				TaskId:       task.TaskID,
				Version:      task.Version,
				FirstEventId: task.FirstEventID,
				NextEventId:  task.NextEventID,
				ScheduledId:  0,
			})
		default:
			panic(fmt.Sprintf("Unknown repication task type: %v", task))
		}
	}

	if len(taskInfo) == 0 {
		return nil, ackLevel, pageToken, nil
	}

	dlqResponse, err := remoteAdminClient.GetDLQReplicationMessages(
		ctx,
		&adminservice.GetDLQReplicationMessagesRequest{
			TaskInfos: taskInfo,
		},
	)
	if err != nil {
		return nil, ackLevel, nil, err
	}

	return dlqResponse.ReplicationTasks, ackLevel, pageToken, nil
}

func (r *dlqHandlerImpl) getOrCreateTaskExecutor(clusterName string) (TaskExecutor, error) {
	r.taskExecutorsLock.Lock()
	defer r.taskExecutorsLock.Unlock()
	if executor, ok := r.taskExecutors[clusterName]; ok {
		return executor, nil
	}
	engine, err := r.shard.GetEngine()
	if err != nil {
		return nil, err
	}
	adminClient := r.shard.GetRemoteAdminClient(clusterName)
	adminRetryableClient := admin.NewRetryableClient(
		adminClient,
		common.CreateReplicationServiceBusyRetryPolicy(),
		common.IsResourceExhausted,
	)
	historyClient := r.shard.GetHistoryClient()
	historyRetryableClient := history.NewRetryableClient(
		historyClient,
		common.CreateReplicationServiceBusyRetryPolicy(),
		common.IsResourceExhausted,
	)
	resender := xdc.NewNDCHistoryResender(
		r.shard.GetNamespaceRegistry(),
		adminRetryableClient,
		func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
			_, err := historyRetryableClient.ReplicateEventsV2(ctx, request)
			return err
		},
		r.shard.GetPayloadSerializer(),
		r.shard.GetConfig().StandbyTaskReReplicationContextTimeout,
		r.shard.GetLogger(),
	)
	taskExecutor := NewTaskExecutor(
		r.shard,
		r.shard.GetNamespaceRegistry(),
		resender,
		engine,
		r.deleteManager,
		r.workflowCache,
		r.shard.GetMetricsClient(),
		r.shard.GetLogger(),
	)
	r.taskExecutors[clusterName] = taskExecutor
	return taskExecutor, nil
}
