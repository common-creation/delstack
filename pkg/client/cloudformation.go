//go:generate mockgen -source=$GOFILE -destination=cloudformation_mock.go -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
)

const CloudFormationWaitNanoSecTime = time.Duration(4500000000000)

// cloudFormationDeletePollInterval is how often waitDeleteStack polls the stack
// status. Overridable by tests.
var cloudFormationDeletePollInterval = 5 * time.Second

type ICloudFormation interface {
	DeleteStack(ctx context.Context, stackName *string, retainResources []string) error
	DescribeStacks(ctx context.Context, stackName *string) ([]types.Stack, error)
	DescribeStackEvents(ctx context.Context, stackName *string) ([]types.StackEvent, error)
	ListStackResources(ctx context.Context, stackName *string) ([]types.StackResourceSummary, error)
	GetTemplate(ctx context.Context, stackName *string) (*string, error)
	UpdateStack(ctx context.Context, stackName *string, templateBody *string, parameters []types.Parameter) error
	UpdateStackWithTemplateURL(ctx context.Context, stackName *string, templateURL *string, parameters []types.Parameter) error
	ListImports(ctx context.Context, exportName *string) ([]string, error)
	DisableTerminationProtection(ctx context.Context, stackName *string) error
}

var _ ICloudFormation = (*CloudFormation)(nil)

type CloudFormation struct {
	client               *cloudformation.Client
	deleteCompleteWaiter *cloudformation.StackDeleteCompleteWaiter
	updateCompleteWaiter *cloudformation.StackUpdateCompleteWaiter
}

func NewCloudFormation(client *cloudformation.Client, deleteCompleteWaiter *cloudformation.StackDeleteCompleteWaiter, updateCompleteWaiter *cloudformation.StackUpdateCompleteWaiter) *CloudFormation {
	return &CloudFormation{
		client,
		deleteCompleteWaiter,
		updateCompleteWaiter,
	}
}

func (c *CloudFormation) DeleteStack(ctx context.Context, stackName *string, retainResources []string) error {
	input := &cloudformation.DeleteStackInput{
		StackName:       stackName,
		RetainResources: retainResources,
	}

	if _, err := c.client.DeleteStack(ctx, input); err != nil {
		return &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	if err := c.waitDeleteStack(ctx, stackName); err != nil {
		return &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	return nil
}

func (c *CloudFormation) DescribeStacks(ctx context.Context, stackName *string) ([]types.Stack, error) {
	var nextToken *string
	stacks := []types.Stack{}

	for {
		select {
		case <-ctx.Done():
			return stacks, &ClientError{
				ResourceName: stackName,
				Err:          ctx.Err(),
			}
		default:
		}

		// If a stackName is nil, then return all stacks
		input := &cloudformation.DescribeStacksInput{
			NextToken: nextToken,
			StackName: stackName,
		}

		output, err := c.client.DescribeStacks(ctx, input)
		if err != nil && strings.Contains(err.Error(), "does not exist") {
			return stacks, nil
		}
		if err != nil {
			return stacks, &ClientError{
				ResourceName: stackName,
				Err:          err,
			}
		}

		if len(stacks) == 0 && len(output.Stacks) == 0 {
			return stacks, nil
		}
		stacks = append(stacks, output.Stacks...)

		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}
	return stacks, nil
}

func (c *CloudFormation) DescribeStackEvents(ctx context.Context, stackName *string) ([]types.StackEvent, error) {
	var nextToken *string
	stackEvents := []types.StackEvent{}

	for {
		select {
		case <-ctx.Done():
			return stackEvents, &ClientError{
				ResourceName: stackName,
				Err:          ctx.Err(),
			}
		default:
		}

		input := &cloudformation.DescribeStackEventsInput{
			StackName: stackName,
			NextToken: nextToken,
		}

		output, err := c.client.DescribeStackEvents(ctx, input)
		if err != nil && strings.Contains(err.Error(), "does not exist") {
			return stackEvents, nil
		}
		if err != nil {
			return stackEvents, &ClientError{
				ResourceName: stackName,
				Err:          err,
			}
		}

		stackEvents = append(stackEvents, output.StackEvents...)

		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}

	return stackEvents, nil
}

func (c *CloudFormation) ListStackResources(ctx context.Context, stackName *string) ([]types.StackResourceSummary, error) {
	var nextToken *string
	stackResourceSummaries := []types.StackResourceSummary{}

	for {
		select {
		case <-ctx.Done():
			return stackResourceSummaries, &ClientError{
				ResourceName: stackName,
				Err:          ctx.Err(),
			}
		default:
		}

		input := &cloudformation.ListStackResourcesInput{
			StackName: stackName,
			NextToken: nextToken,
		}

		output, err := c.client.ListStackResources(ctx, input)
		if err != nil {
			return stackResourceSummaries, &ClientError{
				ResourceName: stackName,
				Err:          err,
			}
		}

		stackResourceSummaries = append(stackResourceSummaries, output.StackResourceSummaries...)
		nextToken = output.NextToken

		if nextToken == nil {
			break
		}
	}

	return stackResourceSummaries, nil
}

func (c *CloudFormation) GetTemplate(ctx context.Context, stackName *string) (*string, error) {
	input := &cloudformation.GetTemplateInput{
		StackName: stackName,
	}

	output, err := c.client.GetTemplate(ctx, input)
	if err != nil {
		return nil, &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	return output.TemplateBody, nil
}

func (c *CloudFormation) UpdateStack(ctx context.Context, stackName *string, templateBody *string, parameters []types.Parameter) error {
	input := &cloudformation.UpdateStackInput{
		StackName:    stackName,
		TemplateBody: templateBody,
		Capabilities: []types.Capability{
			types.CapabilityCapabilityIam,
			types.CapabilityCapabilityNamedIam,
			types.CapabilityCapabilityAutoExpand,
		},
		Parameters: parameters,
	}

	_, err := c.client.UpdateStack(ctx, input)
	if err != nil {
		return &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	if err := c.waitUpdateStack(ctx, stackName); err != nil {
		return &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	return nil
}

// waitDeleteStack polls DescribeStacks until the stack reaches a terminal state.
// The AWS-SDK StackDeleteCompleteWaiter only recognizes DELETE_COMPLETE and
// DELETE_FAILED as terminal, so when CloudFormation cancels a delete and reverts
// the stack to its previous stable state (e.g. "Delete canceled. Cannot delete
// export ... as it is in use by ..."), the waiter keeps polling until the
// 75-minute timeout. This implementation treats any non-delete status after the
// delete was requested as a cancellation and returns immediately.
func (c *CloudFormation) waitDeleteStack(ctx context.Context, stackName *string) error {
	deadline := time.Now().Add(CloudFormationWaitNanoSecTime)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		input := &cloudformation.DescribeStacksInput{
			StackName: stackName,
		}
		output, err := c.client.DescribeStacks(ctx, input)
		if err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				return nil
			}
			return err // return non wrapping error because wrap in public callers
		}
		if len(output.Stacks) == 0 {
			return nil
		}

		status := output.Stacks[0].StackStatus
		switch status {
		case types.StackStatusDeleteComplete:
			return nil
		case types.StackStatusDeleteFailed:
			// Resource-level failures are handled by the caller (retry with
			// RetainResources).
			return nil
		case types.StackStatusDeleteInProgress:
			// Still deleting.
		default:
			return fmt.Errorf(
				"stack %s deletion was canceled by CloudFormation (StackStatus=%s): %s",
				aws.ToString(stackName),
				status,
				aws.ToString(output.Stacks[0].StackStatusReason),
			)
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for stack %s to be deleted", aws.ToString(stackName))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cloudFormationDeletePollInterval):
		}
	}
}

func (c *CloudFormation) ListImports(ctx context.Context, exportName *string) ([]string, error) {
	var nextToken *string
	importingStackNames := []string{}

	for {
		select {
		case <-ctx.Done():
			return importingStackNames, &ClientError{
				ResourceName: exportName,
				Err:          ctx.Err(),
			}
		default:
		}

		input := &cloudformation.ListImportsInput{
			ExportName: exportName,
			NextToken:  nextToken,
		}

		output, err := c.client.ListImports(ctx, input)
		if err != nil {
			// If the export is not imported by any stack, ListImports returns ValidationError
			// This is not an error condition, so return an empty list
			if strings.Contains(err.Error(), "is not imported by any stack") {
				return importingStackNames, nil
			}
			return importingStackNames, &ClientError{
				ResourceName: exportName,
				Err:          err,
			}
		}

		importingStackNames = append(importingStackNames, output.Imports...)

		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}

	return importingStackNames, nil
}

func (c *CloudFormation) DisableTerminationProtection(ctx context.Context, stackName *string) error {
	input := &cloudformation.UpdateTerminationProtectionInput{
		StackName:                   stackName,
		EnableTerminationProtection: aws.Bool(false),
	}

	if _, err := c.client.UpdateTerminationProtection(ctx, input); err != nil {
		return &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	return nil
}

func (c *CloudFormation) waitUpdateStack(ctx context.Context, stackName *string) error {
	input := &cloudformation.DescribeStacksInput{
		StackName: stackName,
	}

	err := c.updateCompleteWaiter.Wait(ctx, input, CloudFormationWaitNanoSecTime)
	if err != nil && !strings.Contains(err.Error(), "waiter state transitioned to Failure") {
		return err // return non wrapping error because wrap in public callers
	}

	return nil
}

func (c *CloudFormation) UpdateStackWithTemplateURL(ctx context.Context, stackName *string, templateURL *string, parameters []types.Parameter) error {
	input := &cloudformation.UpdateStackInput{
		StackName:   stackName,
		TemplateURL: templateURL,
		Capabilities: []types.Capability{
			types.CapabilityCapabilityIam,
			types.CapabilityCapabilityNamedIam,
			types.CapabilityCapabilityAutoExpand,
		},
		Parameters: parameters,
	}

	_, err := c.client.UpdateStack(ctx, input)
	if err != nil {
		return &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	if err := c.waitUpdateStack(ctx, stackName); err != nil {
		return &ClientError{
			ResourceName: stackName,
			Err:          err,
		}
	}

	return nil
}
