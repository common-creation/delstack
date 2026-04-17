package preprocessor

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/delstack/internal/io"
	"github.com/go-to-k/delstack/internal/resourcetype"
	"github.com/go-to-k/delstack/pkg/client"
	"golang.org/x/sync/errgroup"
)

type RecursivePreprocessor struct {
	cfnClient client.ICloudFormation
	pp        IPreprocessor
}

func NewRecursivePreprocessor(cfnClient client.ICloudFormation, pp IPreprocessor) *RecursivePreprocessor {
	return &RecursivePreprocessor{
		cfnClient: cfnClient,
		pp:        pp,
	}
}

func (r *RecursivePreprocessor) PreprocessRecursively(ctx context.Context, stackName *string) error {
	io.Logger.Debug().Msgf("[%v]: PreprocessRecursively: start (listing stack resources)", aws.ToString(stackName))
	resources, err := r.cfnClient.ListStackResources(ctx, stackName)
	if err != nil {
		return fmt.Errorf("failed to list stack resources: %w", err)
	}

	nestedStacks := FilterResourcesByType(resources, resourcetype.CloudformationStack)
	io.Logger.Debug().Msgf("[%v]: PreprocessRecursively: %d resource(s), %d nested stack(s)",
		aws.ToString(stackName), len(resources), len(nestedStacks))

	eg, ctx := errgroup.WithContext(ctx)

	// Process current stack's resources with preprocessor
	eg.Go(func() error {
		err := r.pp.Preprocess(ctx, stackName, resources)
		io.Logger.Debug().Msgf("[%v]: PreprocessRecursively: current stack preprocessor returned (err=%v)",
			aws.ToString(stackName), err)
		return err
	})

	// Process nested stacks recursively in parallel
	for _, nestedStack := range nestedStacks {
		nestedStackName := nestedStack.PhysicalResourceId
		eg.Go(func() error {
			io.Logger.Debug().Msgf("[%v]: Processing nested stack %s", aws.ToString(stackName), aws.ToString(nestedStackName))
			return r.PreprocessRecursively(ctx, nestedStackName)
		})
	}

	err = eg.Wait()
	io.Logger.Debug().Msgf("[%v]: PreprocessRecursively: finished (err=%v)", aws.ToString(stackName), err)
	return err
}
