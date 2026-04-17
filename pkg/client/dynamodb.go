//go:generate mockgen -source=$GOFILE -destination=dynamodb_mock.go -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type IDynamoDB interface {
	CheckTableDeletionProtection(ctx context.Context, tableName *string) (bool, error)
	DisableTableDeletionProtection(ctx context.Context, tableName *string) error
	CheckTableExists(ctx context.Context, tableName *string) (bool, error)
	DeleteTable(ctx context.Context, tableName *string) error
}

var _ IDynamoDB = (*DynamoDB)(nil)

type DynamoDB struct {
	client *dynamodb.Client
}

func NewDynamoDB(client *dynamodb.Client) *DynamoDB {
	return &DynamoDB{
		client: client,
	}
}

func (d *DynamoDB) CheckTableDeletionProtection(ctx context.Context, tableName *string) (bool, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: tableName,
	}

	output, err := d.client.DescribeTable(ctx, input)
	if err != nil {
		var notFound *dynamodbtypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, &ClientError{
			ResourceName: tableName,
			Err:          err,
		}
	}

	if output.Table == nil {
		return false, nil
	}

	return aws.ToBool(output.Table.DeletionProtectionEnabled), nil
}

func (d *DynamoDB) DisableTableDeletionProtection(ctx context.Context, tableName *string) error {
	input := &dynamodb.UpdateTableInput{
		TableName:                 tableName,
		DeletionProtectionEnabled: aws.Bool(false),
	}

	_, err := d.client.UpdateTable(ctx, input)
	if err != nil {
		return &ClientError{
			ResourceName: tableName,
			Err:          err,
		}
	}

	return nil
}

func (d *DynamoDB) CheckTableExists(ctx context.Context, tableName *string) (bool, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: tableName,
	}

	_, err := d.client.DescribeTable(ctx, input)
	if err != nil {
		var notFound *dynamodbtypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, &ClientError{
			ResourceName: tableName,
			Err:          err,
		}
	}

	return true, nil
}

func (d *DynamoDB) DeleteTable(ctx context.Context, tableName *string) error {
	input := &dynamodb.DeleteTableInput{
		TableName: tableName,
	}

	_, err := d.client.DeleteTable(ctx, input)
	if err != nil {
		var notFound *dynamodbtypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return nil
		}
		return &ClientError{
			ResourceName: tableName,
			Err:          err,
		}
	}

	return nil
}
