package operation

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnTypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/go-to-k/delstack/internal/io"
	"github.com/go-to-k/delstack/pkg/client"
	gomock "go.uber.org/mock/gomock"
)

func TestDynamoDBTableOperator_DeleteDynamoDBTable(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx       context.Context
		tableName *string
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIDynamoDB)
		want          error
		wantErr       bool
	}{
		{
			name: "delete dynamodb table without deletion protection",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().CheckTableDeletionProtection(gomock.Any(), aws.String("test")).Return(false, nil)
				m.EXPECT().DeleteTable(gomock.Any(), aws.String("test")).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete dynamodb table with deletion protection",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().CheckTableDeletionProtection(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().DisableTableDeletionProtection(gomock.Any(), aws.String("test")).Return(nil)
				m.EXPECT().DeleteTable(gomock.Any(), aws.String("test")).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "skip delete when table does not exist",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("test")).Return(false, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "fail when check exists errors",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("test")).Return(false, fmt.Errorf("CheckTableExistsError"))
			},
			want:    fmt.Errorf("CheckTableExistsError"),
			wantErr: true,
		},
		{
			name: "fail when check deletion protection errors",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().CheckTableDeletionProtection(gomock.Any(), aws.String("test")).Return(false, fmt.Errorf("CheckTableDeletionProtectionError"))
			},
			want:    fmt.Errorf("CheckTableDeletionProtectionError"),
			wantErr: true,
		},
		{
			name: "fail when disable deletion protection errors",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().CheckTableDeletionProtection(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().DisableTableDeletionProtection(gomock.Any(), aws.String("test")).Return(fmt.Errorf("DisableTableDeletionProtectionError"))
			},
			want:    fmt.Errorf("DisableTableDeletionProtectionError"),
			wantErr: true,
		},
		{
			name: "fail when delete table errors",
			args: args{
				ctx:       context.Background(),
				tableName: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().CheckTableDeletionProtection(gomock.Any(), aws.String("test")).Return(false, nil)
				m.EXPECT().DeleteTable(gomock.Any(), aws.String("test")).Return(fmt.Errorf("DeleteTableError"))
			},
			want:    fmt.Errorf("DeleteTableError"),
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			dynamodbMock := client.NewMockIDynamoDB(ctrl)
			tt.prepareMockFn(dynamodbMock)

			operator := NewDynamoDBTableOperator(dynamodbMock)

			err := operator.DeleteDynamoDBTable(tt.args.ctx, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.Error())
				return
			}
		})
	}
}

func TestDynamoDBTableOperator_DeleteResources(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx context.Context
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIDynamoDB)
		want          error
		wantErr       bool
	}{
		{
			name: "delete resources successfully",
			args: args{
				ctx: context.Background(),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("PhysicalResourceId1")).Return(true, nil)
				m.EXPECT().CheckTableDeletionProtection(gomock.Any(), aws.String("PhysicalResourceId1")).Return(true, nil)
				m.EXPECT().DisableTableDeletionProtection(gomock.Any(), aws.String("PhysicalResourceId1")).Return(nil)
				m.EXPECT().DeleteTable(gomock.Any(), aws.String("PhysicalResourceId1")).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete resources failure",
			args: args{
				ctx: context.Background(),
			},
			prepareMockFn: func(m *client.MockIDynamoDB) {
				m.EXPECT().CheckTableExists(gomock.Any(), aws.String("PhysicalResourceId1")).Return(false, fmt.Errorf("CheckTableExistsError"))
			},
			want:    fmt.Errorf("CheckTableExistsError"),
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			dynamodbMock := client.NewMockIDynamoDB(ctrl)
			tt.prepareMockFn(dynamodbMock)

			operator := NewDynamoDBTableOperator(dynamodbMock)

			operator.AddResource(&cfnTypes.StackResourceSummary{
				LogicalResourceId:  aws.String("LogicalResourceId1"),
				ResourceStatus:     "DELETE_FAILED",
				ResourceType:       aws.String("AWS::DynamoDB::Table"),
				PhysicalResourceId: aws.String("PhysicalResourceId1"),
			})

			err := operator.DeleteResources(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.Error())
				return
			}
		})
	}
}
