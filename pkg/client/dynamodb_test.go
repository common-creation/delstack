package client

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"go.uber.org/goleak"
)

func TestDynamoDB_CheckTableDeletionProtection(t *testing.T) {
	defer goleak.VerifyNone(t)

	type args struct {
		ctx                context.Context
		tableName          *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "check table deletion protection enabled",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable1"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableProtectionEnabledMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{
										Table: &dynamodbtypes.TableDescription{
											DeletionProtectionEnabled: aws.Bool(true),
										},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "check table deletion protection disabled",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable2"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableProtectionDisabledMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{
										Table: &dynamodbtypes.TableDescription{
											DeletionProtectionEnabled: aws.Bool(false),
										},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "check table deletion protection with nil table",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable3"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableNilMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{
										Table: nil,
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "check table deletion protection not found returns false without error",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("NotFoundTable"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableNotFoundMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{},
								}, middleware.Metadata{}, &dynamodbtypes.ResourceNotFoundException{Message: aws.String("not found")}
							},
						),
						middleware.Before,
					)
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "check table deletion protection failure",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable4"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{},
								}, middleware.Metadata{}, fmt.Errorf("DescribeTableError")
							},
						),
						middleware.Before,
					)
				},
			},
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			sdkClient := dynamodb.NewFromConfig(cfg)
			dynamodbClient := NewDynamoDB(sdkClient)

			got, err := dynamodbClient.CheckTableDeletionProtection(tt.args.ctx, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got = %#v, want %#v", got, tt.want)
			}
			if tt.wantErr {
				var clientErr *ClientError
				if !errors.As(err, &clientErr) {
					t.Errorf("expected ClientError, got = %#v", err)
				}
			}
		})
	}
}

func TestDynamoDB_DisableTableDeletionProtection(t *testing.T) {
	defer goleak.VerifyNone(t)

	type args struct {
		ctx                context.Context
		tableName          *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "disable table deletion protection successfully",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable1"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"UpdateTableMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.UpdateTableOutput{},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			wantErr: false,
		},
		{
			name: "disable table deletion protection failure",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable2"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"UpdateTableErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.UpdateTableOutput{},
								}, middleware.Metadata{}, fmt.Errorf("UpdateTableError")
							},
						),
						middleware.Before,
					)
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			sdkClient := dynamodb.NewFromConfig(cfg)
			dynamodbClient := NewDynamoDB(sdkClient)

			err = dynamodbClient.DisableTableDeletionProtection(tt.args.ctx, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				var clientErr *ClientError
				if !errors.As(err, &clientErr) {
					t.Errorf("expected ClientError, got = %#v", err)
				}
			}
		})
	}
}

func TestDynamoDB_CheckTableExists(t *testing.T) {
	defer goleak.VerifyNone(t)

	type args struct {
		ctx                context.Context
		tableName          *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "table exists",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable1"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableExistsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{
										Table: &dynamodbtypes.TableDescription{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "table does not exist",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable2"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableNotFoundMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{},
								}, middleware.Metadata{}, &dynamodbtypes.ResourceNotFoundException{Message: aws.String("not found")}
							},
						),
						middleware.Before,
					)
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "check table exists failure",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable3"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DescribeTableErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DescribeTableOutput{},
								}, middleware.Metadata{}, fmt.Errorf("DescribeTableError")
							},
						),
						middleware.Before,
					)
				},
			},
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			sdkClient := dynamodb.NewFromConfig(cfg)
			dynamodbClient := NewDynamoDB(sdkClient)

			got, err := dynamodbClient.CheckTableExists(tt.args.ctx, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestDynamoDB_DeleteTable(t *testing.T) {
	defer goleak.VerifyNone(t)

	type args struct {
		ctx                context.Context
		tableName          *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "delete table successfully",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable1"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DeleteTableOutput{},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			wantErr: false,
		},
		{
			name: "delete table not found returns nil",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable2"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableNotFoundMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DeleteTableOutput{},
								}, middleware.Metadata{}, &dynamodbtypes.ResourceNotFoundException{Message: aws.String("not found")}
							},
						),
						middleware.Before,
					)
				},
			},
			wantErr: false,
		},
		{
			name: "delete table failure",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("TestTable3"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &dynamodb.DeleteTableOutput{},
								}, middleware.Metadata{}, fmt.Errorf("DeleteTableError")
							},
						),
						middleware.Before,
					)
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			sdkClient := dynamodb.NewFromConfig(cfg)
			dynamodbClient := NewDynamoDB(sdkClient)

			err = dynamodbClient.DeleteTable(tt.args.ctx, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				var clientErr *ClientError
				if !errors.As(err, &clientErr) {
					t.Errorf("expected ClientError, got = %#v", err)
				}
			}
		})
	}
}
