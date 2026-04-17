package operation

import (
	"context"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/go-to-k/delstack/pkg/client"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var _ IOperator = (*DynamoDBTableOperator)(nil)

type DynamoDBTableOperator struct {
	client    client.IDynamoDB
	resources []*types.StackResourceSummary
}

func NewDynamoDBTableOperator(client client.IDynamoDB) *DynamoDBTableOperator {
	return &DynamoDBTableOperator{
		client:    client,
		resources: []*types.StackResourceSummary{},
	}
}

func (o *DynamoDBTableOperator) AddResource(resource *types.StackResourceSummary) {
	o.resources = append(o.resources, resource)
}

func (o *DynamoDBTableOperator) GetResourcesLength() int {
	return len(o.resources)
}

func (o *DynamoDBTableOperator) DeleteResources(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(int64(runtime.NumCPU()))

	for _, resource := range o.resources {
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}
		eg.Go(func() error {
			defer sem.Release(1)

			return o.DeleteDynamoDBTable(ctx, resource.PhysicalResourceId)
		})
	}

	return eg.Wait()
}

func (o *DynamoDBTableOperator) DeleteDynamoDBTable(ctx context.Context, tableName *string) error {
	exists, err := o.client.CheckTableExists(ctx, tableName)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	protected, err := o.client.CheckTableDeletionProtection(ctx, tableName)
	if err != nil {
		return err
	}
	if protected {
		if err := o.client.DisableTableDeletionProtection(ctx, tableName); err != nil {
			return err
		}
	}

	return o.client.DeleteTable(ctx, tableName)
}
